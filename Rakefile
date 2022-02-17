# frozen_string_literal: true

begin
    require 'rspec/core/rake_task'
    RSpec::Core::RakeTask.new(:spec)
rescue LoadError
end

begin
    require 'yard'
    YARD::Rake::YardocTask.new(:doc)
rescue LoadError
end

task default: :spec

