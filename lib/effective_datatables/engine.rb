module EffectiveDatatables
  class Engine < ::Rails::Engine
    engine_name 'effective_datatables'

    config.autoload_paths += Dir["#{config.root}/app/models/concerns"]

    # Include Helpers to base application
    # Helpers in app/helpers are automatically included in Rails engines
    # The application has its own effective_datatables_helper.rb that will be auto-loaded

    # Set up our default configuration options.
    initializer "effective_datatables.defaults", :before => :load_config_initializers do |app|
      eval File.read("#{config.root}/lib/generators/templates/effective_datatables.rb")
    end

  end
end
