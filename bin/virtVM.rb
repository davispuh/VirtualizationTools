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
EFIFB_PATH = Pathname.new('/sys/bus/platform/drivers/efi-framebuffer/')
EFIFB_ID = 'efi-framebuffer.0'

$stderr.sync = true
$stdout.sync = true

class DriverOverrideError < RuntimeError; end
class InvokeError < RuntimeError; end

def self.infoKernelBug
    $stderr.puts("Most likely means we hit a kernel bug, check your dmesg!")
end

def self.writeData(file, data)
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

def self.invokeProgram(*cmd)
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

def self.getVirshCMD(command)
    cmd = ['virsh']
    cmd += ['-c', VIRSH_URI] unless VIRSH_URI.to_s.empty?
    cmd << command
    cmd << VM_NAME
    cmd
end

def self.getVMConfig
    puts("Loading #{VM_NAME} config...")
    cmd = self.getVirshCMD('dumpxml')
    output, status = self.invokeProgram(*cmd)
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

def self.isRegularPCI?(deviceId)
    config = self.getPCIConfig(deviceId)

    config.headerType & TYPE_MASK == 0
end

def self.getIOMMUID(deviceIds)
    (PCI_DEVICE_PATH / deviceIds.first / 'iommu_group').readlink.basename.to_s
end

def self.getVFIODevices(continueOnError = false)
    vmConfig = self.getVMConfig

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
        groupedDevices.select! { |deviceId| self.isRegularPCI?(deviceId) }
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

def self.isVMRunning?
    cmd = self.getVirshCMD('domstate')
    output, status = self.invokeProgram(*cmd)
    if status.success?
        output.strip != 'shut off'
    else
        raise 'Failed to get VM state!'
        false
    end
end


def self.loadModule(name, exitOnError = false)
    puts "Loading #{name} module..."
    output, status = self.invokeProgram('modprobe', name)
    if status.success?
        puts "Module loaded!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed load module!')
        exit(4) if exitOnError
    end
end

def self.unloadModule(name)
    puts "Unloading #{name} module..."
    output, status = self.invokeProgram('rmmod', name)
    if status.success? || /is not currently loaded/.match?(output)
        puts "Module unloaded!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed unload module!')
        exit(5)
    end
end

def self.loadVFIO
    self.loadModule('vfio_pci', true)
end

def self.findGPUs
    gpus = []
    output, status = self.invokeProgram('lspci', '-vnD')
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

def self.stopDisplayServer
    puts "Stopping display server..."
    output, status = self.invokeProgram('systemctl', 'stop', DISPLAY_SERVER)
    if status.success?
        puts "Display server stopped!"
    else
        $stderr.puts(output)
        $stderr.puts('Failed to stop display server!')
        exit(7)
    end
end

def self.startDisplayServer
    puts "Starting display server..."
    output, status = self.invokeProgram('systemctl', 'start', DISPLAY_SERVER)
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

def self.unbindVTconsoles
    console = self.findFBConsole
    if console
        puts "Unbinding #{console.basename}..."
        bindPath = console / 'bind'
        self.writeData(bindPath, '0')
        if File.read(bindPath).strip == '0'
            puts "Unbinded!"
        else
            $stderr.puts('Failed to unbind VT console!')
            exit(8)
        end
    end
end

def self.bindVTconsoles
    console = self.findFBConsole
    if console
        puts "Binding #{console.basename}..."
        success = false
        bindPath = console / 'bind'
        10.times do
            self.writeData(bindPath, '1')
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

def self.unbindEFIFB
    if (EFIFB_PATH / EFIFB_ID).symlink?
        puts "Unbinding EFI framebuffer..."
        self.writeData(EFIFB_PATH / 'unbind', EFIFB_ID)
        puts "Unbinded!"
    else
        puts 'EFI framebuffer not present (probably already unbinded)'
    end
end

def self.bindEFIFB
    puts "Binding EFI framebuffer..."
    begin
        self.writeData(EFIFB_PATH / 'bind', EFIFB_ID)
        puts "Binded!"
    rescue Errno::EINVAL
        $stderr.puts('Failed to bind EFI framebuffer!')
    end
end

def self.unloadModulesAMD
    self.unloadModule('amdgpu')
    self.unloadModule('drm_ttm_helper')
    self.unloadModule('ttm')
    #self.unloadModule('drm_kms_helper')
end

def self.loadModulesAMD
    self.loadModule('amdgpu')
end

def self.unloadGPU(deviceIds)
    gpus = self.findGPUs
    if (deviceIds & gpus).empty?
        puts("No GPU for VM! Won't unload GPU!")
        return
    end
    stopDisplayServer
    unbindVTconsoles
    unbindEFIFB
    unloadModulesAMD
end

def self.loadGPU(deviceIds)
    gpus = (self.findGPUs & deviceIds)
    if gpus.empty?
        puts("No GPU for VM!")
        return
    end
    loadModulesAMD
    gpus.each do |deviceId|
        restoreDriver(deviceId)
    end
    bindEFIFB
    bindVTconsoles
    startDisplayServer
end

def self.getDeviceDriver(deviceId)
    driverFolder = PCI_DEVICE_PATH / deviceId / 'driver'
    return '' unless driverFolder.symlink?
    driverFolder.readlink.basename.to_s
end

def self.probeDriver(deviceId)
    self.writeData(DRIVER_PROBE_PATH, deviceId)
end

def self.rescanPCI()
    self.writeData(PCI_PATH / 'rescan', '1')
end

def self.overrideDriver(deviceId, driver)
    devicePath = PCI_DEVICE_PATH / deviceId
    overridePath = devicePath / 'driver_override'
    self.writeData(overridePath, driver)
    driverPath = devicePath / 'driver'
    if driverPath.symlink?
        unbindPath = driverPath / 'unbind'
        self.writeData(unbindPath, deviceId)
    end
    self.probeDriver(deviceId)
    true
rescue Timeout::Error
    false
end

def self.restoreDriver(deviceId)
    self.overrideDriver(deviceId, "\n")
end

def self.bindVFIO(deviceId)
    currentDriver = self.getDeviceDriver(deviceId)
    if currentDriver != VFIO_PCI_DRIVER
        puts "Overriding device's #{deviceId} driver to #{VFIO_PCI_DRIVER}"

        success = self.overrideDriver(deviceId, VFIO_PCI_DRIVER)

        errorMessage = "Failed to bind #{VFIO_PCI_DRIVER} driver for #{deviceId}!"
        raise DriverOverrideError, errorMessage unless success

        newDriver = self.getDeviceDriver(deviceId)
        raise DriverOverrideError, errorMessage if newDriver != VFIO_PCI_DRIVER && !newDriver.empty?
    else
        puts "Device #{deviceId} is already using #{VFIO_PCI_DRIVER}!"
    end
end

def self.unbindVFIO(deviceId)
    currentDriver = self.getDeviceDriver(deviceId)
    if currentDriver.empty? || currentDriver == VFIO_PCI_DRIVER
        puts "Restoring device's #{deviceId} driver"
        self.restoreDriver(deviceId)
        restoredDriver = self.getDeviceDriver(deviceId)
        $stderr.puts("Failed to unbind driver for #{deviceId}!") if restoredDriver == VFIO_PCI_DRIVER
    else
        puts "Device #{deviceId} is already using correct #{currentDriver}!"
    end
end

def self.bindAll(deviceIds)
    deviceIds.each do |deviceId|
        self.bindVFIO(deviceId)
    end
end

def self.unbindAll(deviceIds)
    deviceIds.each do |deviceId|
        self.unbindVFIO(deviceId)
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

def self.restoreSystem(deviceIds)
    self.unbindAll(deviceIds)
    self.rescanPCI
    self.loadGPU(deviceIds)
rescue SignalException, Timeout::Error => error
    self.handleErrors(error)
end

def self.startVM(attachConsole = true)
    shouldRestoreSystem = false
    groups, deviceIds = self.getVFIODevices(false)

    if self.isVMRunning?
        puts 'VM is already running! Waiting for it to stop...'
        loop do
            sleep(30)
            break unless self.isVMRunning?
        end
    else
        begin
            self.loadVFIO
            shouldRestoreSystem = true
            self.unloadGPU(deviceIds)
            self.bindAll(deviceIds)
            self.rescanPCI

            self.waitForVFIO(groups)

            cmd = self.getVirshCMD('start')
            cmd << '--console' if attachConsole
            s = system(cmd.shelljoin)
            if s && !attachConsole
                loop do
                    sleep(80)
                    break unless self.isVMRunning?
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
    self.restoreSystem(deviceIds) if shouldRestoreSystem
end

def self.stopVM
    begin
        if self.isVMRunning?
            puts "Shutting down #{VM_NAME}..."
            sleep(30) if system(self.getVirshCMD('shutdown').shelljoin)
            if self.isVMRunning?
                puts "VM didn't stop in given time, will force stop!"
                system(self.getVirshCMD('destroy').shelljoin)
                sleep(30)
                $stderr.puts("Failed to stop VM!") if self.isVMRunning?
            end
        else
            puts "#{VM_NAME} is not running!"
        end
    rescue InvokeError
        $stderr.puts("Failed to get VM state!")
    end

    groups, deviceIds = self.getVFIODevices(true)
    self.restoreSystem(deviceIds)
rescue SignalException, Timeout::Error, InvokeError => error
    self.handleErrors(error)
end

def main
    command = ARGV.first
    attachConsole = false
    if command == 'start'
        shouldStop = true
        pid = nil
        begin
            pid = fork do
                self.startVM(attachConsole)
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
        self.stopVM if shouldStop
    elsif command == 'stop'
        self.stopVM
    elsif command == 'directStart'
        self.startVM(attachConsole)
    else
        puts 'Commands: start or stop'
    end
end

main unless $spec
