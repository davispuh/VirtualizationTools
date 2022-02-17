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
DRIVER_PROBE_PATH = '/sys/bus/pci/drivers_probe'
VTCONSOLE_PATH = Pathname.new('/sys/class/vtconsole/')
EFIFB_PATH = Pathname.new('/sys/bus/platform/drivers/efi-framebuffer/')
EFIFB_ID = 'efi-framebuffer.0'

$stderr.sync = true
$stdout.sync = true

def self.infoKernelBug
    $stderr.puts("Most likely means we hit a kernel bug, check your dmesg!")
end

def self.writeData(file, data, ignoreErrors = false)
    Timeout::timeout(TIMEOUT) do
        File.open(file, 'wb') do |io|
            io.write_nonblock(data)
        end
    end
rescue Timeout::Error => error
    raise error unless ignoreErrors
    $stderr.puts("This is taking way too long! Tried to write #{data.inspect} into #{file}")
    self.infoKernelBug
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
    output, status = Open3.capture2e(*cmd)
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
        exit(2)
    end
    true
end

def self.hasIOMMU?(deviceId)
    unless (PCI_DEVICE_PATH / deviceId / 'iommu_group').directory?
        $stderr.puts("It seems IOMMU isn't enabled!")
        exit(3)
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

def self.getVFIODevices
    vmConfig = self.getVMConfig

    deviceIds = self.findPCIDevices(vmConfig)

    if deviceIds.empty?
        $stderr.puts("Warning! VM isn't using any PCI devices!")
    end

    puts 'Looking for PCI device IOMMU groups...'
    iommuGroups = {}
    deviceIds.each do |deviceId|
        groupedDevices = self.findGroupedDevices(deviceId)
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

    vfioDeviceIds
end

def self.isVMRunning?
    cmd = self.getVirshCMD('domstate')
    output, status = Open3.capture2e(*cmd)
    if status.success?
        output.strip != 'shut off'
    else
        $stderr.puts(output)
        $stderr.puts("Failed to get VM state!")
        false
    end
end


def self.loadModule(name, exitOnError = false)
    puts "Loading #{name} module..."
    output, status = Open3.capture2e('modprobe', name)
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
    output, status = Open3.capture2e('rmmod', name)
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
    output, status = Open3.capture2e('lspci', '-vnD')
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
    output, status = Open3.capture2e('systemctl', 'stop', DISPLAY_SERVER)
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
    output, status = Open3.capture2e('systemctl', 'start', DISPLAY_SERVER)
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

def self.probeDriver(deviceId, ignoreErrors = false)
    self.writeData(DRIVER_PROBE_PATH, deviceId, ignoreErrors)
end

def self.rescanPCI(ignoreErrors = false)
    self.writeData(PCI_PATH / 'rescan', '1', ignoreErrors)
end

def self.overrideDriver(deviceId, driver, ignoreErrors = false)
    devicePath = PCI_DEVICE_PATH / deviceId
    overridePath = devicePath / 'driver_override'
    self.writeData(overridePath, driver, ignoreErrors)
    driverPath = devicePath / 'driver'
    if driverPath.symlink?
        unbindPath = driverPath / 'unbind'
        self.writeData(unbindPath, deviceId, ignoreErrors)
    end
    self.probeDriver(deviceId, ignoreErrors)
end

def self.restoreDriver(deviceId)
    self.overrideDriver(deviceId, "\n", true)
end

def self.waitForIOMMU
    '/dev/vfio'
end

def self.bindVFIO(deviceId)
    currentDriver = self.getDeviceDriver(deviceId)
    if currentDriver != VFIO_PCI_DRIVER
        puts "Overriding device's #{deviceId} driver to #{VFIO_PCI_DRIVER}"
        self.overrideDriver(deviceId, VFIO_PCI_DRIVER)
        newDriver = self.getDeviceDriver(deviceId)
        if (newDriver != VFIO_PCI_DRIVER)
            $stderr.puts('Failed to bind driver!')
            exit(9)
        end
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
        if (restoredDriver == VFIO_PCI_DRIVER)
            $stderr.puts('Failed to unbind driver!')
        end
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

def self.startVM(attachConsole = true)
    deviceIds = self.getVFIODevices

    if self.isVMRunning?
        puts 'VM is already running! Waiting for it to stop...'
        loop do
            sleep(30)
            break unless self.isVMRunning?
        end
    else
        begin
            self.loadVFIO
            self.unloadGPU(deviceIds)
            self.bindAll(deviceIds)
            self.rescanPCI

            sleep(5)
            cmd = self.getVirshCMD('start')
            cmd << '--console' if attachConsole
            s = system(cmd.shelljoin)
            if s && !attachConsole
                loop do
                    sleep(80)
                    break unless self.isVMRunning?
                end
            end

        rescue Timeout::Error
            $stderr.puts("This is taking way too long! Aborting!")
            self.showTimeoutError
        end
    end

    self.unbindAll(deviceIds)
    self.rescanPCI(true)
    self.loadGPU(deviceIds)
end

def self.stopVM
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

    deviceIds = self.getVFIODevices
    self.unbindAll(deviceIds)
    self.rescanPCI(true)
    self.loadGPU(deviceIds)
end

def main
    command = ARGV.first
    if command == 'start'
        unless system([RbConfig.ruby, $0, 'directStart'].shelljoin)
            self.stopVM
        end
    elsif command == 'stop'
        self.stopVM
    elsif command == 'directStart'
        self.startVM(false)
    else
        puts 'Commands: start or stop'
    end
end

main unless $spec
