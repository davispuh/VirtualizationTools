#!/usr/bin/env ruby

require 'pathname'
require 'open3'
require 'nokogiri'
require 'ostruct'
require 'shellwords'
require 'timeout'

VM_NAME = 'WindowsVM'
VIRSH_URI = 'qemu:///system'
DISPLAY_SERVER = 'display-manager'

TIMEOUT = 60
PCI_PATH = Pathname.new('/sys/bus/pci')
PCI_DEVICE_PATH = PCI_PATH / 'devices'
VFIO_PCI_DRIVER = 'vfio-pci'
VFIO_PATH = Pathname.new('/dev/vfio')
DRIVER_PROBE_PATH = '/sys/bus/pci/drivers_probe'
VTCONSOLE_PATH = Pathname.new('/sys/class/vtconsole/')
FBDEV_PATTERN = '/dev/fb*'
CHARDEV_PATH = Pathname.new('/sys/dev/char')

$stderr.sync = true
$stdout.sync = true

class DriverOverrideError < RuntimeError; end
class InvokeError < RuntimeError; end

def self.infoKernelBug
    $stderr.puts("Most likely means we hit a kernel bug, check your dmesg!")
end

def self.writeData(file, data, dryRun = false)
    if dryRun
        puts "Would write: echo -n #{data.inspect} > #{file}"
        return
    end
    pid = nil
    Timeout::timeout(TIMEOUT) do
        # We need to fork and write in child
        # because if we hit kernel bug it can lockup forever
        # in that case we just want to abort and not wait eternity
        # note that in such case neither SIGINT nor SIGTERM would work
        # sometimes even SIGKILL doesn't work
        pid = fork do
            File.open(file, 'wb') do |io|
                io.write(data)
            end
        end
        pid, status = Process.wait2(pid)
        pid = nil
        return status.success?
    end
rescue Timeout::Error => error
    $stderr.puts("This is taking way too long!\nTried to write #{data.inspect} into #{file}")
    self.infoKernelBug
    raise error
ensure
    Process.kill(:KILL, pid) if pid
end

def self.invokeProgram(*cmd, **kargs)
    if kargs[:dryRun]
        puts "Would execute: #{cmd.shelljoin}"
        return ['', nil]
    elsif kargs[:debug]
        puts "Will execute: #{cmd.shelljoin}"
    end

    read, write = IO.pipe
    pid = fork do
        read.close

        programPid = nil
        data = Open3.popen2e(*cmd) do |stdin, output, statusWaiter|
            programPid = statusWaiter.pid

            stdin.close
            reader = Thread.new { output.read }
            # we don't want to show confusing exception messages to user so disable this
            reader.report_on_exception = false

            Timeout::timeout(TIMEOUT) do
                [reader.value, statusWaiter.value]
            end
        end
        programPid = nil
        Marshal.dump(data, write)
    rescue SignalException
        exit(100)
    ensure
        write.close
        Process.kill(:TERM, programPid) rescue StandardError if programPid
    end
    write.close

    data = status = nil
    Timeout::timeout(TIMEOUT) do
        data = Marshal.load(read.read)
        pid, status = Process.wait2(pid)
    end
    pid = nil

    raise InvokeError, cmd.join(' ') unless status.success?

    data
rescue Timeout::Error
    raise InvokeError, cmd.join(' ')
ensure
    Process.kill(:INT, pid) if pid
end

def self.executeProgram(cmd, dryRun: false)
    if dryRun
        puts "Would execute: #{cmd.shelljoin}"
        true
    else
        system(cmd.shelljoin)
    end
end

def self.getVirshCMD(*commands, withoutVM: false)
    cmd = ['virsh']
    cmd += ['-c', VIRSH_URI] unless VIRSH_URI.to_s.empty?
    cmd += commands
    cmd << VM_NAME unless withoutVM
    cmd
end

def self.getVMConfig(debug = false)
    puts("Loading #{VM_NAME} config...")
    cmd = self.getVirshCMD('dumpxml')
    output, status = self.invokeProgram(*cmd, debug: debug)
    unless status.success?
        $stderr.puts(output)
        $stderr.puts('Failed to get VM config!')
        exit(1)
    end
    vmConfig = Nokogiri::XML(output)
    puts "Config loaded!"
    vmConfig
end

def self.findPCIDevices(config)
    deviceIds = []
    config.xpath('//devices/hostdev[@type="pci"]/source/address').each do |node|
        params = node.to_h.transform_values { |v| v[2..] }
        deviceIds << [params['domain'], params['bus'], params['slot'] + '.' + params['function']].join(':')
    end
    deviceIds
end

def self.pciExist?(deviceId)
    unless (PCI_DEVICE_PATH / deviceId).directory?
        $stderr.puts("Didn't find #{deviceId} device!")
        return false
    end
    true
end

def self.hasIOMMU?(deviceId)
    unless (PCI_DEVICE_PATH / deviceId / 'iommu_group').directory?
        $stderr.puts("Device #{deviceId} doesn't support IOMMU so can't use it!")
        return false
    end
    true
end

def self.findGroupedDevices(deviceId)
    return unless self.pciExist?(deviceId)
    return unless self.hasIOMMU?(deviceId)
    devicePath = PCI_DEVICE_PATH / deviceId / 'iommu_group' / 'devices'
    devicePath.children
              .select { |device| device.symlink? }
              .map    { |device| device.basename.to_s }
end

def self.getPCIConfig(deviceId)
    return unless self.pciExist?(deviceId)

    fields = %i{vendorId deviceId commandRegister statusRegister revisionId
                subclass classCode cacheLine latencyTimer
                headerType bist}

    data = File.read(PCI_DEVICE_PATH / deviceId / 'config')
    values = data.unpack("S<S<S<S<CCS<CCCC")

    OpenStruct.new(Hash[fields.zip(values)])
end

MULTIPLE_FUNCTIONS_BIT = 1 << 7
TYPE_MASK = MULTIPLE_FUNCTIONS_BIT ^ 0xFF
PCI_GENERAL_DEVICE       = 0x00
PCI_UNINITIALIZED_DEVICE = 0x7F

def self.isUsableForVFIO?(deviceId)
    config = self.getPCIConfig(deviceId)
    deviceType = config.headerType & TYPE_MASK
    # Keep general PCI device (non-bridge)
    # Also keep uninitialized device, this happens in case of kernel bug
    # it won't work but atleast we won't fail when trying to restore drivers
    [PCI_GENERAL_DEVICE, PCI_UNINITIALIZED_DEVICE].include?(deviceType)
end

def self.getIOMMUID(deviceIds)
    (PCI_DEVICE_PATH / deviceIds.first / 'iommu_group').readlink.basename.to_s
end

def self.getVFIODevices(continueOnError = false, debug = false)
    vmConfig = self.getVMConfig(debug)

    deviceIds = self.findPCIDevices(vmConfig)

    if deviceIds.empty?
        $stderr.puts("Warning! VM isn't using any PCI devices!")
    end

    puts 'Looking for PCI device IOMMU groups...'
    iommuGroups = {}
    deviceIds.each do |deviceId|
        groupedDevices = self.findGroupedDevices(deviceId)
        if !groupedDevices
            next if continueOnError
            exit(2)
        end
        # Keep only regular PCI devices, remove PCI-to-PCI Bridges
        groupedDevices.select! { |deviceId| self.isUsableForVFIO?(deviceId) }
        iommuID = self.getIOMMUID(groupedDevices)
        iommuGroups[iommuID] = groupedDevices.sort
    end

    vfioDeviceIds = []
    iommuGroups.keys.sort.each do |iommuID|
        puts "IOMMU group #{iommuID}: " + iommuGroups[iommuID].join(', ')
        vfioDeviceIds += iommuGroups[iommuID]
    end

    [iommuGroups.keys, vfioDeviceIds]
end

def self.isVMRunning?(debug = false)
    cmd = self.getVirshCMD('domstate')
    output, status = self.invokeProgram(*cmd, debug: debug)
    if status.success?
        output.strip != 'shut off'
    else
        raise 'Failed to get VM state!'
        false
    end
end

def self.loadModule(name, exitOnError = false, dryRun = false)
    puts "Loading #{name} module..."
    output, status = self.invokeProgram('modprobe', name, dryRun: dryRun)
    return if dryRun
    if status.success?
        puts "Module loaded!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed load module!')
        exit(4) if exitOnError
    end
end

def self.unloadModule(name, dryRun = false)
    puts "Unloading #{name} module..."
    output, status = self.invokeProgram('rmmod', name, dryRun: dryRun)
    return if dryRun
    if status.success? || /is not currently loaded/.match?(output)
        puts "Module unloaded!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed unload module!')
        exit(5)
    end
end

def self.loadVFIO(dryRun = false)
    self.loadModule('vfio_pci', true, dryRun)
end

def self.findGPUs(debug = false)
    gpus = []
    output, status = self.invokeProgram('lspci', '-vnD', debug: debug)
    if status.success?
        output.each_line do  |line|
            if /\[VGA controller\]/.match?(line)
                gpus << line.split(' ').first.strip
            end
        end
    else
        $stderr.puts(output)
        $stderr.puts('Failed to list PCI devices!')
        exit(6)
    end
    gpus
end

def self.stopDisplayServer(dryRun = false)
    puts "Stopping display server..."
    output, status = self.invokeProgram('systemctl', 'stop', DISPLAY_SERVER, dryRun: dryRun)
    return if dryRun
    if status.success?
        puts "Display server stopped!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed to stop display server!')
        exit(7)
    end
end

def self.startDisplayServer(dryRun = false)
    puts "Starting display server..."
    output, status = self.invokeProgram('systemctl', 'start', DISPLAY_SERVER, dryRun: dryRun)
    return if dryRun
    if status.success?
        puts "Display server started!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed to start display server!')
    end
end

def self.getVTConsoles
    VTCONSOLE_PATH.children
end

def self.findFBConsole
    console = self.getVTConsoles.find do |console|
        /frame buffer device/.match?(File.read(console / 'name'))
    end
    if !console
        $stderr.puts("Didn't find VT console!")
    end
    console
end

def self.unbindVTconsoles(dryRun = false)
    console = self.findFBConsole
    if console
        puts "Unbinding #{console.basename}..."
        bindPath = console / 'bind'
        self.writeData(bindPath, '0', dryRun)
        return if dryRun
        if File.read(bindPath).strip == '0'
            puts "Unbinded!"
        else
            $stderr.puts('Failed to unbind VT console!')
            exit(8)
        end
    end
end

def self.bindVTconsoles(dryRun = false)
    console = self.findFBConsole
    if console
        puts "Binding #{console.basename}..."
        success = false
        bindPath = console / 'bind'
        10.times do
            self.writeData(bindPath, '1', dryRun)
            return if dryRun
            sleep(1)
            success = File.read(bindPath).strip == '1'
            break if success
        end
        if success
            puts "Binded!"
        else
            $stderr.puts('Failed to bind VT console!')
        end
    end
end

def self.getFramebufferDevice(exitOnError = false)
    fbdevs = Dir[FBDEV_PATTERN]
    if fbdevs.empty?
        puts "Didn't find framebuffer!"
        return
    elsif fbdevs.length > 1
        $stderr.puts("Found multiple framebuffers: #{fbdevs.join(',')}")
        $stderr.puts('Currently multiple framebuffer support is not implemented!')
        exit(3) if exitOnError
        return
    end
    puts "Found framebuffer #{fbdevs.first}"
    stat = File.stat(fbdevs.first)
    if stat.chardev?
        id = [stat.rdev_major, stat.rdev_minor].join(':')
        deviceLink = CHARDEV_PATH / id / 'device'
        if deviceLink.symlink?
            deviceLink.readlink.basename.to_s
        else
            $stderr.puts("Expected #{deviceLink} to be a symlink!")
            exit(3) if exitOnError
        end
    else
        $stderr.puts("Expected #{fbdevs.first} to be a character device!")
        exit(3) if exitOnError
    end
end

def self.unbindFramebuffer(dryRun = false)
    fbdev = self.getFramebufferDevice(true)
    return unless fbdev
    puts "Unbinding #{fbdev} framebuffer..."
    self.bindVFIO(fbdev, dryRun)
end

def self.bindFramebuffer(dryRun = false)
    fbdev = self.getFramebufferDevice(false)
    return unless fbdev
    puts "Binding #{fbdev} framebuffer..."
    self.unbindVFIO(fbdev, dryRun)
end

def self.unloadModulesAMD(dryRun = false)
    self.unloadModule('amdgpu', dryRun)
    self.unloadModule('drm_ttm_helper', dryRun)
    self.unloadModule('ttm', dryRun)
    #self.unloadModule('drm_kms_helper', dryRun)
end

def self.loadModulesAMD(dryRun = false)
    self.loadModule('amdgpu', false, dryRun)
end

def self.unloadGPU(deviceIds, dryRun = false)
    gpus = self.findGPUs(dryRun)
    if (deviceIds & gpus).empty?
        puts("No GPU for VM! Won't unload GPU!")
        return
    end
    stopDisplayServer(dryRun)
    unbindVTconsoles(dryRun)
    unloadModulesAMD(dryRun)
end

def self.loadGPU(deviceIds, dryRun = false)
    gpus = (self.findGPUs(dryRun) & deviceIds)
    if gpus.empty?
        puts("No GPU for VM!")
        return
    end
    loadModulesAMD(dryRun)
    bindVTconsoles(dryRun)
    startDisplayServer(dryRun)
end

def self.getDeviceDriver(deviceId)
    driverFolder = PCI_DEVICE_PATH / deviceId / 'driver'
    return '' unless driverFolder.symlink?
    driverFolder.readlink.basename.to_s
end

def self.probeDriver(deviceId, dryRun = false)
    self.writeData(DRIVER_PROBE_PATH, deviceId, dryRun)
end

def self.rescanPCI(dryRun = false)
    self.writeData(PCI_PATH / 'rescan', '1', dryRun)
end

def self.overrideDriver(deviceId, driver, dryRun = false)
    devicePath = PCI_DEVICE_PATH / deviceId
    overridePath = devicePath / 'driver_override'
    self.writeData(overridePath, driver, dryRun)
    driverPath = devicePath / 'driver'
    if driverPath.symlink?
        unbindPath = driverPath / 'unbind'
        self.writeData(unbindPath, deviceId, dryRun)
    end
    self.probeDriver(deviceId, dryRun)
    true
rescue Timeout::Error
    false
end

def self.restoreDriver(deviceId, dryRun = false)
    self.overrideDriver(deviceId, "\n", dryRun)
end

def self.bindVFIO(deviceId, dryRun = false)
    currentDriver = self.getDeviceDriver(deviceId)
    if currentDriver != VFIO_PCI_DRIVER || dryRun
        puts "Overriding device's #{deviceId} driver to #{VFIO_PCI_DRIVER}"

        success = self.overrideDriver(deviceId, VFIO_PCI_DRIVER, dryRun)
        return if dryRun

        errorMessage = "Failed to bind #{VFIO_PCI_DRIVER} driver for #{deviceId}!"
        raise DriverOverrideError, errorMessage unless success

        newDriver = self.getDeviceDriver(deviceId)
        raise DriverOverrideError, errorMessage if newDriver != VFIO_PCI_DRIVER && !newDriver.empty?
    else
        puts "Device #{deviceId} is already using #{VFIO_PCI_DRIVER}!"
    end
end

def self.unbindVFIO(deviceId, dryRun = false)
    currentDriver = self.getDeviceDriver(deviceId)
    if currentDriver.empty? || currentDriver == VFIO_PCI_DRIVER || dryRun
        puts "Restoring device's #{deviceId} driver"
        self.restoreDriver(deviceId, dryRun)
        restoredDriver = self.getDeviceDriver(deviceId)
        return if dryRun
        $stderr.puts("Failed to unbind driver for #{deviceId}!") if restoredDriver == VFIO_PCI_DRIVER
    else
        puts "Device #{deviceId} is already using correct #{currentDriver}!"
    end
end

def self.getVirshDeviceId(deviceId)
    'pci_' + deviceId.gsub(':', '_').gsub('.', '_')
end

def self.detachDevice(deviceId, dryRun = false)
    puts "Detaching #{deviceId} from host"
    cmd = self.getVirshCMD('nodedev-detach', self.getVirshDeviceId(deviceId), withoutVM: true)
    output, status = self.invokeProgram(*cmd, dryRun: dryRun)
    return if dryRun
    if status.success?
        puts "Device #{deviceId} detached!"
    else
        $stderr.puts(output)
        raise DriverOverrideError.new("Failed to detach #{deviceId} device!")
    end
end

def self.reattachDevice(deviceId, dryRun = false)
    puts "Reattaching #{deviceId} to host"
    cmd = self.getVirshCMD('nodedev-reattach', self.getVirshDeviceId(deviceId), withoutVM: true)
    output, status = self.invokeProgram(*cmd, dryRun: dryRun)
    return if dryRun
    if status.success?
        puts "Device #{deviceId} reattached!"
    else
        $stderr.puts(output)
        $stderr.puts("Failed to reattach #{deviceId} device!")
    end
end

def self.bindAll(deviceIds, dryRun = false)
    deviceIds.each do |deviceId|
        self.bindVFIO(deviceId, dryRun)
    end
end

def self.unbindAll(deviceIds, dryRun = false)
    deviceIds.each do |deviceId|
        self.unbindVFIO(deviceId, dryRun)
    end
end

def self.detatchAll(deviceIds, dryRun = false)
    deviceIds.each do |deviceId|
        self.detachDevice(deviceId, dryRun)
    end
end

def self.reattachAll(deviceIds, dryRun = false)
    deviceIds.each do |deviceId|
        self.reattachDevice(deviceId, dryRun)
    end
end

def self.waitForVFIO(groups)
    Timeout::timeout(TIMEOUT) do
        groups.each do |group|
            while !(VFIO_PATH / group).chardev?
                sleep(1)
            end
        end
    end
end

def self.handleErrors(error)
    if error.is_a?(SignalException)
        $stderr.puts(error)
    elsif error.is_a?(Timeout::Error)
        $stderr.puts('Something took too long and we timed out!')
    else
        if error.is_a?(InvokeError)
            $stderr.puts("Failed to execute: #{error.message}")
        else
            $stderr.puts(error)
        end
        $stderr.puts("Error: #{error.cause}") if error.cause
    end
    $stderr.puts('Aborting!')
end

def self.restoreSystem(deviceIds, dryRun = false)
    self.reattachAll(deviceIds, dryRun)
    self.rescanPCI(dryRun)
    self.loadGPU(deviceIds, dryRun)
rescue SignalException, Timeout::Error => error
    self.handleErrors(error)
end

def self.startVM(options)
    shouldRestoreSystem = false
    groups, deviceIds = self.getVFIODevices(false, options[:dryRun])

    if self.isVMRunning?(options[:dryRun])
        puts 'VM is already running! Waiting for it to stop...'
        return if dryRun
        loop do
            sleep(30)
            break unless self.isVMRunning?
        end
    else
        begin
            dryRun = options[:dryRun]
            self.loadVFIO(dryRun)
            shouldRestoreSystem = true
            self.unloadGPU(deviceIds, dryRun)
            self.detatchAll(deviceIds, dryRun)
            cmd = self.getVirshCMD('start')
            cmd << '--console' if options[:attachConsole]
            success = self.executeProgram(cmd, dryRun: dryRun)
            if !dryRun && success && !options[:attachConsole]
                loop do
                    sleep(80)
                    break unless self.isVMRunning?(dryRun)
                end
            end
        rescue DriverOverrideError => error
            $stderr.puts(error)
            $stderr.puts('Aborting!')
        end
    end

rescue SignalException, Timeout::Error, InvokeError => error
    self.handleErrors(error)
ensure
    self.restoreSystem(deviceIds, options[:dryRun]) if shouldRestoreSystem
end

def self.stopVM(options)
    begin
        if self.isVMRunning?(options[:dryRun])
            puts "Shutting down #{VM_NAME}..."
            success = self.executeProgram(self.getVirshCMD('shutdown'), dryRun: options[:dryRun])
            if !options[:dryRun]
                sleep(30) if success
                if self.isVMRunning?(options[:dryRun])
                    puts "VM didn't stop in given time, will force stop!"
                    self.executeProgram(self.getVirshCMD('destroy'), dryRun: options[:dryRun])
                    sleep(30)
                    $stderr.puts("Failed to stop VM!") if self.isVMRunning?(options[:dryRun])
                end
            end
        else
            puts "#{VM_NAME} is not running!"
        end
    rescue InvokeError
        $stderr.puts("Failed to get VM state!")
    end

    groups, deviceIds = self.getVFIODevices(true, options[:dryRun])
    self.restoreSystem(deviceIds, options[:dryRun])
rescue SignalException, Timeout::Error, InvokeError => error
    self.handleErrors(error)
end

def main
    command = ARGV.first
    options = {
        attachConsole: false,
        dryRun: false
    }
    if command == 'start'
        shouldStop = true
        pid = nil
        begin
            pid = fork do
                self.startVM(options)
            end
            pid, status = Process.wait2(pid)
            shouldStop = !status.success?
        rescue Interrupt
            begin
                Timeout::timeout(TIMEOUT + 5) do
                    pid, status = Process.wait2(pid)
                    shouldStop = !status.success?
                end
            rescue Timeout::Error
                # Just abort and try to stop
            rescue Interrupt
                # Was aborted by user
                shouldStop = false
            ensure
                Process.kill(:KILL, pid) rescue StandardError if pid
            end
        end
        self.stopVM(options) if shouldStop
    elsif command == 'stop'
        self.stopVM(options)
    elsif command == 'directStart'
        self.startVM(options)
    else
        puts 'Commands: start or stop'
    end
end

main unless $spec
