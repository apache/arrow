# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module Helper
  class PlasmaStore
    def initialize(options={})
      @path = `pkg-config --variable=executable plasma`.chomp
      @memory_size = options[:memory_size] || 1024 * 1024
      @socket_file = Tempfile.new(["plasma-store", ".sock"])
      @socket_file.close
      @pid = nil
      FileUtils.rm_f(socket_path)
    end

    def socket_path
      @socket_file.path
    end

    def start
      @pid = spawn(@path,
                   "-m", @memory_size.to_s,
                   "-s", socket_path)
      until File.exist?(socket_path)
        if Process.waitpid(@pid, Process::WNOHANG)
          raise "Failed to run plasma_store_server: #{@path}"
        end
      end
    end

    def stop
      return if @pid.nil?
      Process.kill(:TERM, @pid)
      timeout = 1
      limit = Time.now + timeout
      while Time.now < limit
        return if Process.waitpid(@pid, Process::WNOHANG)
        sleep(0.1)
      end
      Process.kill(:KILL, @pid)
      Process.waitpid(@pid)
    end
  end
end
