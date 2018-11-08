module Plasma
  class Client
    alias_method :initialize_raw, :initialize
    def initialize(socket_path)
      socket_path = socket_path.to_path if socket_path.respond_to?(:to_path)
      initialize_raw(socket_path)
    end
  end
end
