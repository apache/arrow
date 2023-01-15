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

require "cgi/util"
require "digest/sha2"
require "io/console"
require "json"
require "net/http"
require "pathname"
require "tempfile"
require "thread"
require "time"

begin
  require "apt-dists-merge"
rescue LoadError
  warn("apt-dists-merge is needed for apt:* tasks")
end

class BinaryTask
  include Rake::DSL

  class ThreadPool
    def initialize(use_case, &worker)
      @n_workers = choose_n_workers(use_case)
      @worker = worker
      @jobs = Thread::Queue.new
      @workers = @n_workers.times.collect do
        Thread.new do
          loop do
            job = @jobs.pop
            break if job.nil?
            @worker.call(job)
          end
        end
      end
    end

    def <<(job)
      @jobs << job
    end

    def join
      @n_workers.times do
        @jobs << nil
      end
      @workers.each(&:join)
    end

    private
    def choose_n_workers(use_case)
      case use_case
      when :artifactory
        # Too many workers cause Artifactory error.
        6
      when :gpg
        # Too many workers cause gpg-agent error.
        2
      else
        raise "Unknown use case: #{use_case}"
      end
    end
  end

  class ProgressReporter
    def initialize(label, count_max=0)
      @label = label
      @count_max = count_max

      @mutex = Thread::Mutex.new

      @time_start = Time.now
      @time_previous = Time.now
      @count_current = 0
      @count_previous = 0
    end

    def advance
      @mutex.synchronize do
        @count_current += 1

        return if @count_max.zero?

        time_current = Time.now
        if time_current - @time_previous <= 1
          return
        end

        show_progress(time_current)
      end
    end

    def increment_max
      @mutex.synchronize do
        @count_max += 1
        show_progress(Time.now) if @count_max == 1
      end
    end

    def finish
      @mutex.synchronize do
        return if @count_max.zero?
        show_progress(Time.now)
        $stderr.puts
      end
    end

    private
    def show_progress(time_current)
      n_finishes = @count_current - @count_previous
      throughput = n_finishes.to_f / (time_current - @time_previous)
      @time_previous = time_current
      @count_previous = @count_current

      message = build_message(time_current, throughput)
      $stderr.print("\r#{message}") if message
    end

    def build_message(time_current, throughput)
      percent = (@count_current / @count_max.to_f) * 100
      formatted_count = "[%s/%s]" % [
        format_count(@count_current),
        format_count(@count_max),
      ]
      elapsed_second = time_current - @time_start
      if throughput.zero?
        rest_second = 0
      else
        rest_second = (@count_max - @count_current) / throughput
      end
      separator = " - "
      progress = "%5.1f%% %s %s %s %s" % [
        percent,
        formatted_count,
        format_time_interval(elapsed_second),
        format_time_interval(rest_second),
        format_throughput(throughput),
      ]
      label = @label

      width = guess_terminal_width
      return "#{label}#{separator}#{progress}" if width.nil?

      return nil if progress.size > width

      label_width = width - progress.size - separator.size
      if label.size > label_width
        ellipsis = "..."
        shorten_label_width = label_width - ellipsis.size
        if shorten_label_width < 1
          return progress
        else
          label = label[0, shorten_label_width] + ellipsis
        end
      end
      "#{label}#{separator}#{progress}"
    end

    def format_count(count)
      "%d" % count
    end

    def format_time_interval(interval)
      if interval < 60
        "00:00:%02d" % interval
      elsif interval < (60 * 60)
        minute, second = interval.divmod(60)
        "00:%02d:%02d" % [minute, second]
      elsif interval < (60 * 60 * 24)
        minute, second = interval.divmod(60)
        hour, minute = minute.divmod(60)
        "%02d:%02d:%02d" % [hour, minute, second]
      else
        minute, second = interval.divmod(60)
        hour, minute = minute.divmod(60)
        day, hour = hour.divmod(24)
        "%dd %02d:%02d:%02d" % [day, hour, minute, second]
      end
    end

    def format_throughput(throughput)
      "%2d/s" % throughput
    end

    def guess_terminal_width
      guess_terminal_width_from_io ||
        guess_terminal_width_from_command ||
        guess_terminal_width_from_env ||
        80
    end

    def guess_terminal_width_from_io
      if IO.respond_to?(:console) and IO.console
        IO.console.winsize[1]
      elsif $stderr.respond_to?(:winsize)
        begin
          $stderr.winsize[1]
        rescue SystemCallError
          nil
        end
      else
        nil
      end
    end

    def guess_terminal_width_from_command
      IO.pipe do |input, output|
        begin
          pid = spawn("tput", "cols", {:out => output, :err => output})
        rescue SystemCallError
          return nil
        end

        output.close
        _, status = Process.waitpid2(pid)
        return nil unless status.success?

        result = input.read.chomp
        begin
          Integer(result, 10)
        rescue ArgumentError
          nil
        end
      end
    end

    def guess_terminal_width_from_env
      env = ENV["COLUMNS"] || ENV["TERM_WIDTH"]
      return nil if env.nil?

      begin
        Integer(env, 10)
      rescue ArgumentError
        nil
      end
    end
  end

  class ArtifactoryClient
    class Error < StandardError
      attr_reader :request
      attr_reader :response
      def initialize(request, response, message)
        @request = request
        @response = response
        super(message)
      end
    end

    def initialize(prefix, api_key)
      @prefix = prefix
      @api_key = api_key
      @http = nil
      restart
    end

    def restart
      close
      @http = start_http(build_url(""))
    end

    private def start_http(url, &block)
      http = Net::HTTP.new(url.host, url.port)
      http.set_debug_output($stderr) if ENV["DEBUG"]
      http.use_ssl = true
      if block_given?
        http.start(&block)
      else
        http
      end
    end

    def close
      return if @http.nil?
      @http.finish if @http.started?
      @http = nil
    end

    def request(method, headers, url, body: nil, &block)
      request = build_request(method, url, headers, body: body)
      if ENV["DRY_RUN"] == "yes"
        case request
        when Net::HTTP::Get, Net::HTTP::Head
        else
          p [method, url]
          return
        end
      end
      request_internal(@http, request, &block)
    end

    private def request_internal(http, request, &block)
      http.request(request) do |response|
        case response
        when Net::HTTPSuccess,
             Net::HTTPNotModified
          if block_given?
            return yield(response)
          else
            response.read_body
            return response
          end
        when Net::HTTPRedirection
          redirected_url = URI(response["Location"])
          redirected_request = Net::HTTP::Get.new(redirected_url, {})
          start_http(redirected_url) do |redirected_http|
            request_internal(redirected_http, redirected_request, &block)
          end
        else
          message = "failed to request: "
          message << "#{request.uri}: #{request.method}: "
          message << "#{response.message} #{response.code}"
          if response.body
            message << "\n"
            message << response.body
          end
          raise Error.new(request, response, message)
        end
      end
    end

    def files
      _files = []
      directories = [""]
      until directories.empty?
        directory = directories.shift
        list(directory).each do |path|
          resolved_path = "#{directory}#{path}"
          case path
          when "../"
          when /\/\z/
            directories << resolved_path
          else
            _files << resolved_path
          end
        end
      end
      _files
    end

    def list(path)
      url = build_url(path)
      with_retry(3, url) do
        begin
          request(:get, {}, url) do |response|
            response.body.scan(/<a href="(.+?)"/).flatten
          end
        rescue Error => error
          case error.response
          when Net::HTTPNotFound
            return []
          else
            raise
          end
        end
      end
    end

    def head(path)
      url = build_url(path)
      with_retry(3, url) do
        request(:head, {}, url)
      end
    end

    def exist?(path)
      begin
        head(path)
        true
      rescue Error => error
        case error.response
        when Net::HTTPNotFound
          false
        else
          raise
        end
      end
    end

    def upload(path, destination_path)
      destination_url = build_url(destination_path)
      with_retry(3, destination_url) do
        sha1 = Digest::SHA1.file(path).hexdigest
        sha256 = Digest::SHA256.file(path).hexdigest
        headers = {
          "X-Artifactory-Last-Modified" => File.mtime(path).rfc2822,
          "X-Checksum-Deploy" => "false",
          "X-Checksum-Sha1" => sha1,
          "X-Checksum-Sha256" => sha256,
          "Content-Length" => File.size(path).to_s,
          "Content-Type" => "application/octet-stream",
        }
        File.open(path, "rb") do |input|
          request(:put, headers, destination_url, body: input)
        end
      end
    end

    def download(path, output_path=nil)
      url = build_url(path)
      with_retry(5, url) do
        begin
          begin
            headers = {}
            if output_path and File.exist?(output_path)
              headers["If-Modified-Since"] = File.mtime(output_path).rfc2822
            end
            request(:get, headers, url) do |response|
              case response
              when Net::HTTPNotModified
              else
                if output_path
                  File.open(output_path, "wb") do |output|
                    response.read_body do |chunk|
                      output.write(chunk)
                    end
                  end
                  last_modified = response["Last-Modified"]
                  if last_modified
                    FileUtils.touch(output_path,
                                    mtime: Time.rfc2822(last_modified))
                  end
                else
                  response.body
                end
              end
            end
          rescue Error => error
            case error.response
            when Net::HTTPNotFound
              $stderr.puts(error.message)
              return
            else
              raise
            end
          end
        end
      rescue
        FileUtils.rm_f(output_path)
        raise
      end
    end

    def delete(path)
      url = build_url(path)
      with_retry(3, url) do
        request(:delete, {}, url)
      end
    end

    def copy(source, destination)
      url = build_api_url("copy/arrow/#{source}",
                          "to" => "/arrow/#{destination}")
      with_retry(3, url) do
        with_read_timeout(300) do
          request(:post, {}, url)
        end
      end
    end

    private
    def build_url(path)
      uri_string = "https://apache.jfrog.io/artifactory/arrow"
      uri_string << "/#{@prefix}" unless @prefix.nil?
      uri_string << "/#{path}"
      URI(uri_string)
    end

    def build_api_url(path, parameters)
      uri_string = "https://apache.jfrog.io/artifactory/api/#{path}"
      unless parameters.empty?
        uri_string << "?"
        escaped_parameters = parameters.collect do |key, value|
          "#{CGI.escape(key)}=#{CGI.escape(value)}"
        end
        uri_string << escaped_parameters.join("&")
      end
      URI(uri_string)
    end

    def build_request(method, url, headers, body: nil)
      need_auth = false
      case method
      when :head
        request = Net::HTTP::Head.new(url, headers)
      when :get
        request = Net::HTTP::Get.new(url, headers)
      when :post
        need_auth = true
        request = Net::HTTP::Post.new(url, headers)
      when :put
        need_auth = true
        request = Net::HTTP::Put.new(url, headers)
      when :delete
        need_auth = true
        request = Net::HTTP::Delete.new(url, headers)
      else
        raise "unsupported HTTP method: #{method.inspect}"
      end
      request["Connection"] = "Keep-Alive"
      request["X-JFrog-Art-Api"] = @api_key if need_auth
      if body
        if body.is_a?(String)
          request.body = body
        else
          request.body_stream = body
        end
      end
      request
    end

    def with_retry(max_n_retries, target)
      n_retries = 0
      begin
        yield
      rescue Net::OpenTimeout,
             OpenSSL::OpenSSLError,
             SocketError,
             SystemCallError,
             Timeout::Error => error
        n_retries += 1
        if n_retries <= max_n_retries
          $stderr.puts
          $stderr.puts("Retry #{n_retries}: #{target}: " +
                       "#{error.class}: #{error.message}")
          restart
          retry
        else
          raise
        end
      end
    end

    def with_read_timeout(timeout)
      current_timeout = @http.read_timeout
      begin
        @http.read_timeout = timeout
        yield
      ensure
        @http.read_timeout = current_timeout
      end
    end
  end

  class ArtifactoryClientPool
    class << self
      def open(prefix, api_key)
        pool = new(prefix, api_key)
        begin
          yield(pool)
        ensure
          pool.close
        end
      end
    end

    def initialize(prefix, api_key)
      @prefix = prefix
      @api_key = api_key
      @mutex = Thread::Mutex.new
      @clients = []
    end

    def pull
      client = @mutex.synchronize do
        if @clients.empty?
          ArtifactoryClient.new(@prefix, @api_key)
        else
          @clients.pop
        end
      end
      begin
        yield(client)
      ensure
        release(client)
      end
    end

    def release(client)
      @mutex.synchronize do
        @clients << client
      end
    end

    def close
      @clients.each(&:close)
    end
  end

  module ArtifactoryPath
    private
    def base_path
      path = @distribution
      path += "-staging" if @staging
      path
    end

    def rc_base_path
      base_path + "-rc"
    end

    def release_base_path
      base_path
    end

    def target_base_path
      if @rc
        rc_base_path
      else
        release_base_path
      end
    end
  end

  class ArtifactoryDownloader
    include ArtifactoryPath

    def initialize(api_key:,
                   destination:,
                   distribution:,
                   pattern: nil,
                   prefix: nil,
                   rc: nil,
                   staging: false)
      @api_key = api_key
      @destination = destination
      @distribution = distribution
      @pattern = pattern
      @prefix = prefix
      @rc = rc
      @staging = staging
    end

    def download
      progress_label = "Downloading: #{target_base_path}"
      progress_reporter = ProgressReporter.new(progress_label)
      prefix = [target_base_path, @prefix].compact.join("/")
      ArtifactoryClientPool.open(prefix, @api_key) do |client_pool|
        thread_pool = ThreadPool.new(:artifactory) do |path, output_path|
          client_pool.pull do |client|
            client.download(path, output_path)
          end
          progress_reporter.advance
        end
        files = client_pool.pull do |client|
          client.files
        end
        files.each do |path|
          output_path = "#{@destination}/#{path}"
          if @pattern
            next unless @pattern.match?(path)
          end
          yield(output_path)
          output_dir = File.dirname(output_path)
          FileUtils.mkdir_p(output_dir)
          progress_reporter.increment_max
          thread_pool << [path, output_path]
        end
        thread_pool.join
      end
      progress_reporter.finish
    end
  end

  class ArtifactoryUploader
    include ArtifactoryPath

    def initialize(api_key:,
                   destination_prefix: nil,
                   distribution:,
                   rc: nil,
                   source:,
                   staging: false,
                   sync: false,
                   sync_pattern: nil)
      @api_key = api_key
      @destination_prefix = destination_prefix
      @distribution = distribution
      @rc = rc
      @source = source
      @staging = staging
      @sync = sync
      @sync_pattern = sync_pattern
    end

    def upload
      progress_label = "Uploading: #{target_base_path}"
      progress_reporter = ProgressReporter.new(progress_label)
      prefix = target_base_path
      prefix += "/#{@destination_prefix}" if @destination_prefix
      ArtifactoryClientPool.open(prefix, @api_key) do |client_pool|
        if @sync
          existing_files = client_pool.pull do |client|
            client.files
          end
        else
          existing_files = []
        end

        thread_pool = ThreadPool.new(:artifactory) do |path, relative_path|
          client_pool.pull do |client|
            client.upload(path, relative_path)
          end
          progress_reporter.advance
        end

        source = Pathname(@source)
        source.glob("**/*") do |path|
          next if path.directory?
          destination_path = path.relative_path_from(source)
          progress_reporter.increment_max
          existing_files.delete(destination_path.to_s)
          thread_pool << [path, destination_path]
        end
        thread_pool.join

        if @sync
          thread_pool = ThreadPool.new(:artifactory) do |path|
            client_pool.pull do |client|
              client.delete(path)
            end
            progress_reporter.advance
          end
          existing_files.each do |path|
            if @sync_pattern
              next unless @sync_pattern.match?(path)
            end
            progress_reporter.increment_max
            thread_pool << path
          end
          thread_pool.join
        end
      end
      progress_reporter.finish
    end
  end

  class ArtifactoryReleaser
    include ArtifactoryPath

    def initialize(api_key:,
                   distribution:,
                   list: nil,
                   rc_prefix: nil,
                   release_prefix: nil,
                   staging: false)
      @api_key = api_key
      @distribution = distribution
      @list = list
      @rc_prefix = rc_prefix
      @release_prefix = release_prefix
      @staging = staging
    end

    def release
      progress_label = "Releasing: #{release_base_path}"
      progress_reporter = ProgressReporter.new(progress_label)
      rc_prefix = [rc_base_path, @rc_prefix].compact.join("/")
      release_prefix = [release_base_path, @release_prefix].compact.join("/")
      ArtifactoryClientPool.open(rc_prefix, @api_key) do |client_pool|
        thread_pool = ThreadPool.new(:artifactory) do |path, release_path|
          client_pool.pull do |client|
            client.copy(path, release_path)
          end
          progress_reporter.advance
        end
        files = client_pool.pull do |client|
          if @list
            client.download(@list, nil).lines(chomp: true)
          else
            client.files
          end
        end
        files.each do |path|
          progress_reporter.increment_max
          rc_path = "#{rc_prefix}/#{path}"
          release_path = "#{release_prefix}/#{path}"
          thread_pool << [rc_path, release_path]
        end
        thread_pool.join
      end
      progress_reporter.finish
    end
  end

  def define
    define_apt_tasks
    define_yum_tasks
    define_docs_tasks
    define_nuget_tasks
    define_python_tasks
    define_r_tasks
    define_summary_tasks
  end

  private
  def env_value(name)
    value = ENV[name]
    value = yield(name) if value.nil? and block_given?
    raise "Specify #{name} environment variable" if value.nil?
    value
  end

  def verbose?
    ENV["VERBOSE"] == "yes"
  end

  def default_output
    if verbose?
      $stdout
    else
      IO::NULL
    end
  end

  def gpg_key_id
    env_value("GPG_KEY_ID")
  end

  def shorten_gpg_key_id(id)
    id[-8..-1]
  end

  def rpm_gpg_key_package_name(id)
    "gpg-pubkey-#{shorten_gpg_key_id(id).downcase}"
  end

  def artifactory_api_key
    env_value("ARTIFACTORY_API_KEY")
  end

  def artifacts_dir
    env_value("ARTIFACTS_DIR")
  end

  def version
    env_value("VERSION")
  end

  def rc
    env_value("RC")
  end

  def staging?
    ENV["STAGING"] == "yes"
  end

  def full_version
    "#{version}-rc#{rc}"
  end

  def valid_sign?(path, sign_path)
    IO.pipe do |input, output|
      begin
        sh({"LANG" => "C"},
           "gpg",
           "--verify",
           sign_path,
           path,
           out: default_output,
           err: output,
           verbose: false)
      rescue
        return false
      end
      output.close
      /Good signature/ === input.read
    end
  end

  def sign(source_path, destination_path)
    if File.exist?(destination_path)
      return if valid_sign?(source_path, destination_path)
      rm(destination_path, verbose: false)
    end
    sh("gpg",
       "--armor",
       "--detach-sign",
       "--local-user", gpg_key_id,
       "--output", destination_path,
       source_path,
       out: default_output,
       verbose: verbose?)
  end

  def sha512(source_path, destination_path)
    if File.exist?(destination_path)
      sha512 = File.read(destination_path).split[0]
      return if Digest::SHA512.file(source_path).hexdigest == sha512
    end
    absolute_destination_path = File.expand_path(destination_path)
    Dir.chdir(File.dirname(source_path)) do
      sh("shasum",
         "--algorithm", "512",
         File.basename(source_path),
         out: absolute_destination_path,
         verbose: verbose?)
    end
  end

  def sign_dir(label, dir)
    progress_label = "Signing: #{label}"
    progress_reporter = ProgressReporter.new(progress_label)

    target_paths = []
    Pathname(dir).glob("**/*") do |path|
      next if path.directory?
      case path.extname
      when ".asc", ".sha512"
        next
      end
      progress_reporter.increment_max
      target_paths << path.to_s
    end
    target_paths.each do |path|
      sign(path, "#{path}.asc")
      sha512(path, "#{path}.sha512")
      progress_reporter.advance
    end
    progress_reporter.finish
  end

  def download_distribution(distribution,
                            destination,
                            target,
                            pattern: nil,
                            prefix: nil)
    mkdir_p(destination, verbose: verbose?) unless File.exist?(destination)
    existing_paths = {}
    Pathname(destination).glob("**/*") do |path|
      next if path.directory?
      existing_paths[path.to_s] = true
    end
    options = {
      api_key: artifactory_api_key,
      destination: destination,
      distribution: distribution,
      pattern: pattern,
      prefix: prefix,
      staging: staging?,
    }
    options[:rc] = rc if target == :rc
    downloader = ArtifactoryDownloader.new(**options)
    downloader.download do |output_path|
      existing_paths.delete(output_path)
    end
    existing_paths.each_key do |path|
      rm_f(path, verbose: verbose?)
    end
  end

  def release_distribution(distribution,
                           list: nil,
                           rc_prefix: nil,
                           release_prefix: nil)
    options = {
      api_key: artifactory_api_key,
      distribution: distribution,
      list: list,
      rc_prefix: rc_prefix,
      release_prefix: release_prefix,
      staging: staging?,
    }
    releaser = ArtifactoryReleaser.new(**options)
    releaser.release
  end

  def same_content?(path1, path2)
    File.exist?(path1) and
      File.exist?(path2) and
      Digest::SHA256.file(path1) == Digest::SHA256.file(path2)
  end

  def copy_artifact(source_path,
                    destination_path,
                    progress_reporter)
    return if same_content?(source_path, destination_path)
    progress_reporter.increment_max
    destination_dir = File.dirname(destination_path)
    unless File.exist?(destination_dir)
      mkdir_p(destination_dir, verbose: verbose?)
    end
    cp(source_path, destination_path, verbose: verbose?)
    progress_reporter.advance
  end

  def prepare_staging(base_path)
    client = ArtifactoryClient.new(nil, artifactory_api_key)
    ["", "-rc"].each do |suffix|
      path = "#{base_path}#{suffix}"
      progress_reporter = ProgressReporter.new("Preparing staging for #{path}")
      progress_reporter.increment_max
      begin
        staging_path = "#{base_path}-staging#{suffix}"
        if client.exist?(staging_path)
          client.delete(staging_path)
        end
        if client.exist?(path)
          client.copy(path, staging_path)
        end
      ensure
        progress_reporter.advance
        progress_reporter.finish
      end
    end
  end

  def delete_staging(base_path)
    client = ArtifactoryClient.new(nil, artifactory_api_key)
    ["", "-rc"].each do |suffix|
      path = "#{base_path}#{suffix}"
      progress_reporter = ProgressReporter.new("Deleting staging for #{path}")
      progress_reporter.increment_max
      begin
        staging_path = "#{base_path}-staging#{suffix}"
        if client.exist?(staging_path)
          client.delete(staging_path)
        end
      ensure
        progress_reporter.advance
        progress_reporter.finish
      end
    end
  end

  def uploaded_files_name
    "uploaded-files.txt"
  end

  def write_uploaded_files(dir)
    dir = Pathname(dir)
    uploaded_files = []
    dir.glob("**/*") do |path|
      next if path.directory?
      uploaded_files << path.relative_path_from(dir).to_s
    end
    File.open("#{dir}/#{uploaded_files_name}", "w") do |output|
      output.puts(uploaded_files.sort)
    end
  end

  def tmp_dir
    "/tmp"
  end

  def rc_dir
    "#{tmp_dir}/rc"
  end

  def release_dir
    "#{tmp_dir}/release"
  end

  def apt_repository_label
    "Apache Arrow"
  end

  def apt_repository_description
    "Apache Arrow packages"
  end

  def apt_rc_repositories_dir
    "#{rc_dir}/apt/repositories"
  end

  def apt_release_repositories_dir
    "#{release_dir}/apt/repositories"
  end

  def available_apt_targets
    [
      ["debian", "bullseye", "main"],
      ["debian", "bookworm", "main"],
      ["ubuntu", "bionic", "main"],
      ["ubuntu", "focal", "main"],
      ["ubuntu", "jammy", "main"],
      ["ubuntu", "kinetic", "main"],
    ]
  end

  def apt_targets
    env_apt_targets = (ENV["APT_TARGETS"] || "").split(",")
    if env_apt_targets.empty?
      available_apt_targets
    else
      available_apt_targets.select do |distribution, code_name, component|
        env_apt_targets.any? do |env_apt_target|
          if env_apt_target.include?("-")
            env_apt_target.start_with?("#{distribution}-#{code_name}")
          else
            env_apt_target == distribution
          end
        end
      end
    end
  end

  def apt_distributions
    apt_targets.collect(&:first).uniq
  end

  def apt_architectures
    [
      "amd64",
      "arm64",
    ]
  end

  def generate_apt_release(dists_dir, code_name, component, architecture)
    dir = "#{dists_dir}/#{component}/"
    if architecture == "source"
      dir << architecture
    else
      dir << "binary-#{architecture}"
    end

    mkdir_p(dir, verbose: verbose?)
    File.open("#{dir}/Release", "w") do |release|
      release.puts(<<-RELEASE)
Archive: #{code_name}
Component: #{component}
Origin: #{apt_repository_label}
Label: #{apt_repository_label}
Architecture: #{architecture}
      RELEASE
    end
  end

  def generate_apt_ftp_archive_generate_conf(code_name, component)
    conf = <<-CONF
Dir::ArchiveDir ".";
Dir::CacheDir ".";
TreeDefault::Directory "pool/#{code_name}/#{component}";
TreeDefault::SrcDirectory "pool/#{code_name}/#{component}";
Default::Packages::Extensions ".deb";
Default::Packages::Compress ". gzip xz";
Default::Sources::Compress ". gzip xz";
Default::Contents::Compress "gzip";
    CONF

    apt_architectures.each do |architecture|
      conf << <<-CONF

BinDirectory "dists/#{code_name}/#{component}/binary-#{architecture}" {
  Packages "dists/#{code_name}/#{component}/binary-#{architecture}/Packages";
  Contents "dists/#{code_name}/#{component}/Contents-#{architecture}";
  SrcPackages "dists/#{code_name}/#{component}/source/Sources";
};
      CONF
    end

    conf << <<-CONF

Tree "dists/#{code_name}" {
  Sections "#{component}";
  Architectures "#{apt_architectures.join(" ")} source";
};
    CONF

    conf
  end

  def generate_apt_ftp_archive_release_conf(code_name, component)
    <<-CONF
APT::FTPArchive::Release::Origin "#{apt_repository_label}";
APT::FTPArchive::Release::Label "#{apt_repository_label}";
APT::FTPArchive::Release::Architectures "#{apt_architectures.join(" ")}";
APT::FTPArchive::Release::Codename "#{code_name}";
APT::FTPArchive::Release::Suite "#{code_name}";
APT::FTPArchive::Release::Components "#{component}";
APT::FTPArchive::Release::Description "#{apt_repository_description}";
    CONF
  end

  def apt_update(base_dir, incoming_dir, merged_dir)
    apt_targets.each do |distribution, code_name, component|
      distribution_dir = "#{incoming_dir}/#{distribution}"
      pool_dir = "#{distribution_dir}/pool/#{code_name}"
      next unless File.exist?(pool_dir)
      dists_dir = "#{distribution_dir}/dists/#{code_name}"
      rm_rf(dists_dir, verbose: verbose?)
      generate_apt_release(dists_dir, code_name, component, "source")
      apt_architectures.each do |architecture|
        generate_apt_release(dists_dir, code_name, component, architecture)
      end

      generate_conf_file = Tempfile.new("apt-ftparchive-generate.conf")
      File.open(generate_conf_file.path, "w") do |conf|
        conf.puts(generate_apt_ftp_archive_generate_conf(code_name,
                                                         component))
      end
      cd(distribution_dir, verbose: verbose?) do
        sh("apt-ftparchive",
           "generate",
           generate_conf_file.path,
           out: default_output,
           verbose: verbose?)
      end

      Dir.glob("#{dists_dir}/Release*") do |release|
        rm_f(release, verbose: verbose?)
      end
      Dir.glob("#{distribution_dir}/*.db") do |db|
        rm_f(db, verbose: verbose?)
      end
      release_conf_file = Tempfile.new("apt-ftparchive-release.conf")
      File.open(release_conf_file.path, "w") do |conf|
        conf.puts(generate_apt_ftp_archive_release_conf(code_name,
                                                        component))
      end
      release_file = Tempfile.new("apt-ftparchive-release")
      sh("apt-ftparchive",
         "-c", release_conf_file.path,
         "release",
         dists_dir,
         out: release_file.path,
         verbose: verbose?)
      mv(release_file.path, "#{dists_dir}/Release", verbose: verbose?)

      base_dists_dir = "#{base_dir}/#{distribution}/dists/#{code_name}"
      merged_dists_dir = "#{merged_dir}/#{distribution}/dists/#{code_name}"
      rm_rf(merged_dists_dir)
      merger = APTDistsMerge::Merger.new(base_dists_dir,
                                         dists_dir,
                                         merged_dists_dir)
      merger.merge

      in_release_path = "#{merged_dists_dir}/InRelease"
      release_path = "#{merged_dists_dir}/Release"
      signed_release_path = "#{release_path}.gpg"
      sh("gpg",
         "--sign",
         "--detach-sign",
         "--armor",
         "--local-user", gpg_key_id,
         "--output", signed_release_path,
         release_path,
         out: default_output,
         verbose: verbose?)
      sh("gpg",
         "--clear-sign",
         "--local-user", gpg_key_id,
         "--output", in_release_path,
         release_path,
         out: default_output,
         verbose: verbose?)
    end
  end

  def define_apt_staging_tasks
    namespace :apt do
      namespace :staging do
        desc "Prepare staging environment for APT repositories"
        task :prepare do
          apt_distributions.each do |distribution|
            prepare_staging(distribution)
          end
        end

        desc "Delete staging environment for APT repositories"
        task :delete do
          apt_distributions.each do |distribution|
            delete_staging(distribution)
          end
        end
      end
    end
  end

  def define_apt_rc_tasks
    namespace :apt do
      namespace :rc do
        base_dir = "#{apt_rc_repositories_dir}/base"
        incoming_dir = "#{apt_rc_repositories_dir}/incoming"
        merged_dir = "#{apt_rc_repositories_dir}/merged"
        upload_dir = "#{apt_rc_repositories_dir}/upload"

        desc "Copy .deb packages"
        task :copy do
          apt_targets.each do |distribution, code_name, component|
            progress_label = "Copying: #{distribution} #{code_name}"
            progress_reporter = ProgressReporter.new(progress_label)

            distribution_dir = "#{incoming_dir}/#{distribution}"
            pool_dir = "#{distribution_dir}/pool/#{code_name}"
            rm_rf(pool_dir, verbose: verbose?)
            mkdir_p(pool_dir, verbose: verbose?)
            source_dir_prefix = "#{artifacts_dir}/#{distribution}-#{code_name}"
            Dir.glob("#{source_dir_prefix}*/**/*") do |path|
              next if File.directory?(path)
              base_name = File.basename(path)
              package_name = ENV["DEB_PACKAGE_NAME"]
              if package_name.nil? or package_name.empty?
                if base_name.start_with?("apache-arrow-apt-source")
                  package_name = "apache-arrow-apt-source"
                else
                  package_name = "apache-arrow"
                end
              end
              destination_path = [
                pool_dir,
                component,
                package_name[0],
                package_name,
                base_name,
              ].join("/")
              copy_artifact(path,
                            destination_path,
                            progress_reporter)
              case base_name
              when /\A[^_]+-apt-source_.*\.deb\z/
                latest_apt_source_package_path = [
                  distribution_dir,
                  "#{package_name}-latest-#{code_name}.deb"
                ].join("/")
                copy_artifact(path,
                              latest_apt_source_package_path,
                              progress_reporter)
              end
            end
            progress_reporter.finish
          end
        end

        desc "Download dists/ for RC APT repositories"
        task :download do
          apt_distributions.each do |distribution|
            not_checksum_pattern = /.+(?<!\.asc|\.sha512)\z/
            base_distribution_dir = "#{base_dir}/#{distribution}"
            pattern = /\Adists\/#{not_checksum_pattern}/
            download_distribution(distribution,
                                  base_distribution_dir,
                                  :base,
                                  pattern: pattern)
          end
        end

        desc "Sign .deb packages"
        task :sign do
          apt_distributions.each do |distribution|
            distribution_dir = "#{incoming_dir}/#{distribution}"
            Dir.glob("#{distribution_dir}/**/*.dsc") do |path|
              begin
                sh({"LANG" => "C"},
                   "gpg",
                   "--verify",
                   path,
                   out: IO::NULL,
                   err: IO::NULL,
                   verbose: false)
              rescue
                sh("debsign",
                   "--no-re-sign",
                   "-k#{gpg_key_id}",
                   path,
                   out: default_output,
                   verbose: verbose?)
              end
            end
            sign_dir(distribution, distribution_dir)
          end
        end

        desc "Update RC APT repositories"
        task :update do
          apt_update(base_dir, incoming_dir, merged_dir)
          apt_targets.each do |distribution, code_name, component|
            dists_dir = "#{merged_dir}/#{distribution}/dists/#{code_name}"
            next unless File.exist?(dists_dir)
            sign_dir("#{distribution} #{code_name}",
                     dists_dir)
          end
        end

        desc "Upload .deb packages and RC APT repositories"
        task :upload do
          apt_distributions.each do |distribution|
            upload_distribution_dir = "#{upload_dir}/#{distribution}"
            incoming_distribution_dir = "#{incoming_dir}/#{distribution}"
            merged_dists_dir = "#{merged_dir}/#{distribution}/dists"

            rm_rf(upload_distribution_dir, verbose: verbose?)
            mkdir_p(upload_distribution_dir, verbose: verbose?)
            Dir.glob("#{incoming_distribution_dir}/*") do |path|
              next if File.basename(path) == "dists"
              cp_r(path,
                   upload_distribution_dir,
                   preserve: true,
                   verbose: verbose?)
            end
            cp_r(merged_dists_dir,
                 upload_distribution_dir,
                 preserve: true,
                 verbose: verbose?)
            write_uploaded_files(upload_distribution_dir)
            uploader = ArtifactoryUploader.new(api_key: artifactory_api_key,
                                               distribution: distribution,
                                               rc: rc,
                                               source: upload_distribution_dir,
                                               staging: staging?)
            uploader.upload
          end
        end
      end

      desc "Release RC APT repositories"
      apt_rc_tasks = [
        "apt:rc:copy",
        "apt:rc:download",
        "apt:rc:sign",
        "apt:rc:update",
        "apt:rc:upload",
      ]
      apt_rc_tasks.unshift("apt:staging:prepare") if staging?
      task :rc => apt_rc_tasks
    end
  end

  def define_apt_release_tasks
    directory apt_release_repositories_dir

    namespace :apt do
      task :release do
        apt_distributions.each do |distribution|
          release_distribution(distribution,
                               list: uploaded_files_name)
        end
      end
    end
  end

  def define_apt_tasks
    define_apt_staging_tasks
    define_apt_rc_tasks
    define_apt_release_tasks
  end

  def yum_rc_repositories_dir
    "#{rc_dir}/yum/repositories"
  end

  def yum_release_repositories_dir
    "#{release_dir}/yum/repositories"
  end

  def available_yum_targets
    [
      ["almalinux", "9"],
      ["almalinux", "8"],
      ["amazon-linux", "2"],
      ["centos", "9-stream"],
      ["centos", "8-stream"],
      ["centos", "7"],
    ]
  end

  def yum_targets
    env_yum_targets = (ENV["YUM_TARGETS"] || "").split(",")
    if env_yum_targets.empty?
      available_yum_targets
    else
      available_yum_targets.select do |distribution, distribution_version|
        env_yum_targets.any? do |env_yum_target|
          if /\d/.match?(env_yum_target)
            env_yum_target.start_with?("#{distribution}-#{distribution_version}")
          else
            env_yum_target == distribution
          end
        end
      end
    end
  end

  def yum_distributions
    yum_targets.collect(&:first).uniq
  end

  def yum_architectures
    [
      "aarch64",
      "x86_64",
    ]
  end

  def signed_rpm?(rpm)
    IO.pipe do |input, output|
      system("rpm", "--checksig", rpm, out: output)
      output.close
      signature = input.gets.sub(/\A#{Regexp.escape(rpm)}: /, "")
      signature.split.include?("signatures")
    end
  end

  def sign_rpms(directory)
    thread_pool = ThreadPool.new(:gpg) do |rpm|
      unless signed_rpm?(rpm)
        sh("rpm",
           "-D", "_gpg_name #{gpg_key_id}",
           "-D", "__gpg /usr/bin/gpg",
           "-D", "__gpg_check_password_cmd /bin/true true",
           "--resign",
           rpm,
           out: default_output,
           verbose: verbose?)
      end
    end
    Dir.glob("#{directory}/**/*.rpm") do |rpm|
      thread_pool << rpm
    end
    thread_pool.join
  end

  def rpm_sign(directory)
    unless system("rpm", "-q",
                  rpm_gpg_key_package_name(gpg_key_id),
                  out: IO::NULL)
      gpg_key = Tempfile.new(["apache-arrow-binary", ".asc"])
      sh("gpg",
         "--armor",
         "--export", gpg_key_id,
         out: gpg_key.path,
         verbose: verbose?)
      sh("rpm",
         "--import", gpg_key.path,
         out: default_output,
         verbose: verbose?)
      gpg_key.close!
    end

    yum_targets.each do |distribution, distribution_version|
      source_dir = [
        directory,
        distribution,
        distribution_version,
      ].join("/")
      sign_rpms(source_dir)
    end
  end

  def yum_update(base_dir, incoming_dir)
    yum_targets.each do |distribution, distribution_version|
      target_dir = "#{incoming_dir}/#{distribution}/#{distribution_version}"
      target_dir = Pathname(target_dir)
      next unless target_dir.directory?

      base_target_dir = Pathname(base_dir) + distribution + distribution_version
      if base_target_dir.exist?
        base_target_dir.glob("*") do |base_arch_dir|
          next unless base_arch_dir.directory?

          base_repodata_dir = base_arch_dir + "repodata"
          next unless base_repodata_dir.exist?

          target_repodata_dir = target_dir + base_arch_dir.basename + "repodata"
          rm_rf(target_repodata_dir, verbose: verbose?)
          mkdir_p(target_repodata_dir.parent, verbose: verbose?)
          cp_r(base_repodata_dir,
               target_repodata_dir,
               preserve: true,
               verbose: verbose?)
        end
      end

      target_dir.glob("*") do |arch_dir|
        next unless arch_dir.directory?

        packages = Tempfile.new("createrepo-c-packages")
        Pathname.glob("#{arch_dir}/*/*.rpm") do |rpm|
          relative_rpm = rpm.relative_path_from(arch_dir)
          packages.puts(relative_rpm.to_s)
        end
        packages.close
        sh("createrepo_c",
           "--pkglist", packages.path,
           "--recycle-pkglist",
           "--retain-old-md-by-age=0",
           "--skip-stat",
           "--update",
           arch_dir.to_s,
           out: default_output,
           verbose: verbose?)
      end
    end
  end

  def define_yum_staging_tasks
    namespace :yum do
      namespace :staging do
        desc "Prepare staging environment for Yum repositories"
        task :prepare do
          yum_distributions.each do |distribution|
            prepare_staging(distribution)
          end
        end

        desc "Delete staging environment for Yum repositories"
        task :delete do
          yum_distributions.each do |distribution|
            delete_staging(distribution)
          end
        end
      end
    end
  end

  def define_yum_rc_tasks
    namespace :yum do
      namespace :rc do
        base_dir = "#{yum_rc_repositories_dir}/base"
        incoming_dir = "#{yum_rc_repositories_dir}/incoming"
        upload_dir = "#{yum_rc_repositories_dir}/upload"

        desc "Copy RPM packages"
        task :copy do
          yum_targets.each do |distribution, distribution_version|
            progress_label = "Copying: #{distribution} #{distribution_version}"
            progress_reporter = ProgressReporter.new(progress_label)

            destination_prefix = [
              incoming_dir,
              distribution,
              distribution_version,
            ].join("/")
            rm_rf(destination_prefix, verbose: verbose?)
            source_dir_prefix =
              "#{artifacts_dir}/#{distribution}-#{distribution_version}"
            Dir.glob("#{source_dir_prefix}*/**/*") do |path|
              next if File.directory?(path)
              base_name = File.basename(path)
              type = base_name.split(".")[-2]
              destination_paths = []
              case type
              when "src"
                destination_paths << [
                  destination_prefix,
                  "Source",
                  "SPackages",
                  base_name,
                ].join("/")
              when "noarch"
                yum_architectures.each do |architecture|
                  destination_paths << [
                    destination_prefix,
                    architecture,
                    "Packages",
                    base_name,
                  ].join("/")
                end
              else
                destination_paths << [
                  destination_prefix,
                  type,
                  "Packages",
                  base_name,
                ].join("/")
              end
              destination_paths.each do |destination_path|
                copy_artifact(path,
                              destination_path,
                              progress_reporter)
              end
              case base_name
              when /\A(apache-arrow-release)-.*\.noarch\.rpm\z/
                package_name = $1
                latest_release_package_path = [
                  destination_prefix,
                  "#{package_name}-latest.rpm"
                ].join("/")
                copy_artifact(path,
                              latest_release_package_path,
                              progress_reporter)
              end
            end

            progress_reporter.finish
          end
        end

        desc "Download repodata for RC Yum repositories"
        task :download do
          yum_distributions.each do |distribution|
            distribution_dir = "#{base_dir}/#{distribution}"
            download_distribution(distribution,
                                  distribution_dir,
                                  :base,
                                  pattern: /\/repodata\//)
          end
        end

        desc "Sign RPM packages"
        task :sign do
          rpm_sign(incoming_dir)
          yum_targets.each do |distribution, distribution_version|
            source_dir = [
              incoming_dir,
              distribution,
              distribution_version,
            ].join("/")
            sign_dir("#{distribution}-#{distribution_version}",
                     source_dir)
          end
        end

        desc "Update RC Yum repositories"
        task :update do
          yum_update(base_dir, incoming_dir)
          yum_targets.each do |distribution, distribution_version|
            target_dir = [
              incoming_dir,
              distribution,
              distribution_version,
            ].join("/")
            target_dir = Pathname(target_dir)
            next unless target_dir.directory?
            target_dir.glob("*") do |arch_dir|
              next unless arch_dir.directory?
              sign_label =
                "#{distribution}-#{distribution_version} #{arch_dir.basename}"
              sign_dir(sign_label,
                       arch_dir.to_s)
            end
          end
        end

        desc "Upload RC Yum repositories"
        task :upload => yum_rc_repositories_dir do
          yum_distributions.each do |distribution|
            incoming_target_dir = "#{incoming_dir}/#{distribution}"
            upload_target_dir = "#{upload_dir}/#{distribution}"

            rm_rf(upload_target_dir, verbose: verbose?)
            mkdir_p(upload_target_dir, verbose: verbose?)
            cp_r(Dir.glob("#{incoming_target_dir}/*"),
                 upload_target_dir.to_s,
                 preserve: true,
                 verbose: verbose?)
            write_uploaded_files(upload_target_dir)

            uploader = ArtifactoryUploader.new(api_key: artifactory_api_key,
                                               distribution: distribution,
                                               rc: rc,
                                               source: upload_target_dir,
                                               staging: staging?,
                                               sync: true,
                                               sync_pattern: /\/repodata\//)
            uploader.upload
          end
        end
      end

      desc "Release RC Yum packages"
      yum_rc_tasks = [
        "yum:rc:copy",
        "yum:rc:download",
        "yum:rc:sign",
        "yum:rc:update",
        "yum:rc:upload",
      ]
      yum_rc_tasks.unshift("yum:staging:prepare") if staging?
      task :rc => yum_rc_tasks
    end
  end

  def define_yum_release_tasks
    directory yum_release_repositories_dir

    namespace :yum do
      desc "Release Yum packages"
      task :release => yum_release_repositories_dir do
        yum_distributions.each do |distribution|
          release_distribution(distribution,
                               list: uploaded_files_name)

          # Remove old repodata
          distribution_dir = "#{yum_release_repositories_dir}/#{distribution}"
          download_distribution(distribution,
                                distribution_dir,
                                :rc,
                                pattern: /\/repodata\//)
          uploader = ArtifactoryUploader.new(api_key: artifactory_api_key,
                                             distribution: distribution,
                                             source: distribution_dir,
                                             staging: staging?,
                                             sync: true,
                                             sync_pattern: /\/repodata\//)
          uploader.upload
        end
      end
    end
  end

  def define_yum_tasks
    define_yum_staging_tasks
    define_yum_rc_tasks
    define_yum_release_tasks
  end

  def define_generic_data_rc_tasks(label,
                                   id,
                                   rc_dir,
                                   target_files_glob)
    directory rc_dir

    namespace id do
      namespace :rc do
        desc "Copy #{label} packages"
        task :copy => rc_dir do
          progress_label = "Copying: #{label}"
          progress_reporter = ProgressReporter.new(progress_label)

          Pathname(artifacts_dir).glob(target_files_glob) do |path|
            next if path.directory?
            destination_path = [
              rc_dir,
              path.basename.to_s,
            ].join("/")
            copy_artifact(path, destination_path, progress_reporter)
          end

          progress_reporter.finish
        end

        desc "Sign #{label} packages"
        task :sign => rc_dir do
          sign_dir(label, rc_dir)
        end

        desc "Upload #{label} packages"
        task :upload do
          uploader =
            ArtifactoryUploader.new(api_key: artifactory_api_key,
                                    destination_prefix: full_version,
                                    distribution: id.to_s,
                                    rc: rc,
                                    source: rc_dir,
                                    staging: staging?)
          uploader.upload
        end
      end

      desc "Release RC #{label} packages"
      rc_tasks = [
        "#{id}:rc:copy",
        "#{id}:rc:sign",
        "#{id}:rc:upload",
      ]
      task :rc => rc_tasks
    end
  end

  def define_generic_data_release_tasks(label, id, release_dir)
    directory release_dir

    namespace id do
      desc "Release #{label} packages"
      task :release do
        release_distribution(id.to_s,
                             rc_prefix: full_version,
                             release_prefix: version)
      end
    end
  end

  def define_generic_data_tasks(label,
                                id,
                                rc_dir,
                                release_dir,
                                target_files_glob)
    define_generic_data_rc_tasks(label, id, rc_dir, target_files_glob)
    define_generic_data_release_tasks(label, id, release_dir)
  end

  def define_docs_tasks
    define_generic_data_tasks("Docs",
                              :docs,
                              "#{rc_dir}/docs/#{full_version}",
                              "#{release_dir}/docs/#{full_version}",
                              "test-ubuntu-default-docs/**/*")
  end

  def define_nuget_tasks
    define_generic_data_tasks("NuGet",
                              :nuget,
                              "#{rc_dir}/nuget/#{full_version}",
                              "#{release_dir}/nuget/#{full_version}",
                              "nuget/**/*")
  end

  def define_python_tasks
    define_generic_data_tasks("Python",
                              :python,
                              "#{rc_dir}/python/#{full_version}",
                              "#{release_dir}/python/#{full_version}",
                              "{python-sdist,wheel-*}/**/*")
  end

  def define_r_rc_tasks(label, id, rc_dir)
    directory rc_dir

    namespace id do
      namespace :rc do
        desc "Prepare #{label} packages"
        task :prepare => rc_dir do
          progress_label = "Preparing #{label}"
          progress_reporter = ProgressReporter.new(progress_label)

          pattern = "r-binary-packages/r-lib*.{zip,tgz}"
          Pathname(artifacts_dir).glob(pattern) do |path|
            destination_path = [
              rc_dir,
              # r-lib__libarrow__bin__centos-7__arrow-8.0.0.zip
              # --> libarrow/bin/centos-7/arrow-8.0.0.zip
              path.basename.to_s.gsub(/\Ar-lib__/, "").gsub(/__/, "/"),
            ].join("/")
            copy_artifact(path, destination_path, progress_reporter)
          end

          progress_reporter.finish
        end

        desc "Sign #{label} packages"
        task :sign => rc_dir do
          sign_dir(label, rc_dir)
        end

        desc "Upload #{label} packages"
        task :upload do
          uploader =
            ArtifactoryUploader.new(api_key: artifactory_api_key,
                                    destination_prefix: full_version,
                                    distribution: id.to_s,
                                    rc: rc,
                                    source: rc_dir,
                                    staging: staging?)
          uploader.upload
        end
      end

      desc "Release RC #{label} packages"
      rc_tasks = [
        "#{id}:rc:prepare",
        "#{id}:rc:sign",
        "#{id}:rc:upload",
      ]
      task :rc => rc_tasks
    end
  end

  def define_r_tasks
    label = "R"
    id = :r
    r_rc_dir = "#{rc_dir}/r/#{full_version}"
    r_release_dir = "#{release_dir}/r/#{full_version}"
    define_r_rc_tasks(label, id, r_rc_dir)
    define_generic_data_release_tasks(label, id, r_release_dir)
  end

  def define_summary_tasks
    namespace :summary do
      desc "Show RC summary"
      task :rc do
        suffix = ""
        suffix << "-staging" if staging?
        puts(<<-SUMMARY)
Success! The release candidate binaries are available here:
  https://apache.jfrog.io/artifactory/arrow/almalinux#{suffix}-rc/
  https://apache.jfrog.io/artifactory/arrow/amazon-linux#{suffix}-rc/
  https://apache.jfrog.io/artifactory/arrow/centos#{suffix}-rc/
  https://apache.jfrog.io/artifactory/arrow/debian#{suffix}-rc/
  https://apache.jfrog.io/artifactory/arrow/docs#{suffix}-rc/
  https://apache.jfrog.io/artifactory/arrow/nuget#{suffix}-rc/#{full_version}
  https://apache.jfrog.io/artifactory/arrow/python#{suffix}-rc/#{full_version}
  https://apache.jfrog.io/artifactory/arrow/r#{suffix}-rc/#{full_version}
  https://apache.jfrog.io/artifactory/arrow/ubuntu#{suffix}-rc/
        SUMMARY
      end

      desc "Show release summary"
      task :release do
        suffix = ""
        suffix << "-staging" if staging?
        puts(<<-SUMMARY)
Success! The release binaries are available here:
  https://apache.jfrog.io/artifactory/arrow/almalinux#{suffix}/
  https://apache.jfrog.io/artifactory/arrow/amazon-linux#{suffix}/
  https://apache.jfrog.io/artifactory/arrow/centos#{suffix}/
  https://apache.jfrog.io/artifactory/arrow/debian#{suffix}/
  https://apache.jfrog.io/artifactory/arrow/docs#{suffix}/
  https://apache.jfrog.io/artifactory/arrow/nuget#{suffix}/#{version}
  https://apache.jfrog.io/artifactory/arrow/python#{suffix}/#{version}
  https://apache.jfrog.io/artifactory/arrow/r#{suffix}/#{version}
  https://apache.jfrog.io/artifactory/arrow/ubuntu#{suffix}/
        SUMMARY
      end
    end
  end
end

class LocalBinaryTask < BinaryTask
  def initialize(packages, top_source_directory)
    @packages = packages
    @top_source_directory = top_source_directory
    super()
  end

  def define
    define_apt_test_task
    define_yum_test_task
  end

  private
  def resolve_docker_image(target)
    case target
    when /-(?:arm64|aarch64)\z/
      target = Regexp.last_match.pre_match
      platform = "linux/arm64"
    else
      platform = "linux/amd64"
    end

    case target
    when /\Acentos-(\d+)-stream\z/
      centos_stream_version = $1
      image = "quay.io/centos/centos:stream#{centos_stream_version}"
    else
      case platform
      when "linux/arm64"
        image = "arm64v8/"
      else
        image = ""
      end
      target = target.gsub(/\Aamazon-linux/, "amazonlinux")
      image << target.gsub(/-/, ":")
    end

    [platform, image]
  end

  def verify_apt_sh
    "/host/dev/release/verify-apt.sh"
  end

  def verify_yum_sh
    "/host/dev/release/verify-yum.sh"
  end

  def verify(target)
    verify_command_line = [
      "docker",
      "run",
      "--log-driver", "none",
      "--rm",
      "--security-opt", "seccomp=unconfined",
      "--volume", "#{@top_source_directory}:/host:delegated",
    ]
    if $stdin.tty?
      verify_command_line << "--interactive"
      verify_command_line << "--tty"
    else
      verify_command_line.concat(["--attach", "STDOUT"])
      verify_command_line.concat(["--attach", "STDERR"])
    end
    platform, docker_image = resolve_docker_image(target)
    docker_info = JSON.parse(`docker info --format '{{json .}}'`)
    case [platform, docker_info["Architecture"]]
    when ["linux/amd64", "x86_64"],
         ["linux/arm64", "aarch64"]
      # Do nothing
    else
      verify_command_line.concat(["--platform", platform])
    end
    verify_command_line << docker_image
    case target
    when /\Adebian-/, /\Aubuntu-/
      verify_command_line << verify_apt_sh
    else
      verify_command_line << verify_yum_sh
    end
    verify_command_line << version
    verify_command_line << "local"
    sh(*verify_command_line)
  end

  def apt_test_targets
    targets = (ENV["APT_TARGETS"] || "").split(",")
    targets = apt_test_targets_default if targets.empty?
    targets
  end

  def apt_test_targets_default
    # Disable arm64 targets by default for now
    # because they require some setups on host.
    [
      "debian-buster",
      # "debian-buster-arm64",
      "debian-bullseye",
      # "debian-bullseye-arm64",
      "debian-bookworm",
      # "debian-bookworm-arm64",
      "ubuntu-bionic",
      # "ubuntu-bionic-arm64",
      "ubuntu-focal",
      # "ubuntu-focal-arm64",
      "ubuntu-impish",
      # "ubuntu-impish-arm64",
    ]
  end

  def define_apt_test_task
    namespace :apt do
      desc "Test deb packages"
      task :test do
        repositories_dir = "apt/repositories"
        unless @packages.empty?
          rm_rf(repositories_dir)
          @packages.each do |package|
            package_repositories = "#{package}/apt/repositories"
            next unless File.exist?(package_repositories)
            sh("rsync", "-av", "#{package_repositories}/", repositories_dir)
          end
        end
        Dir.glob("#{repositories_dir}/ubuntu/pool/*") do |code_name_dir|
          universe_dir = "#{code_name_dir}/universe"
          next unless File.exist?(universe_dir)
          mv(universe_dir, "#{code_name_dir}/main")
        end
        base_dir = "nonexistent"
        merged_dir = "apt/merged"
        apt_update(base_dir, repositories_dir, merged_dir)
        Dir.glob("#{merged_dir}/*/dists/*") do |dists_code_name_dir|
          prefix = dists_code_name_dir.split("/")[-3..-1].join("/")
          mv(Dir.glob("#{dists_code_name_dir}/*Release*"),
             "#{repositories_dir}/#{prefix}")
        end
        apt_test_targets.each do |target|
          verify(target)
        end
      end
    end
  end

  def yum_test_targets
    targets = (ENV["YUM_TARGETS"] || "").split(",")
    targets = yum_test_targets_default if targets.empty?
    targets
  end

  def yum_test_targets_default
    # Disable aarch64 targets by default for now
    # because they require some setups on host.
    [
      "almalinux-9",
      # "almalinux-9-aarch64",
      "almalinux-8",
      # "almalinux-8-aarch64",
      "amazon-linux-2",
      # "amazon-linux-2-aarch64",
      "centos-9-stream",
      # "centos-9-stream-aarch64",
      "centos-8-stream",
      # "centos-8-stream-aarch64",
      "centos-7",
      # "centos-7-aarch64",
    ]
  end

  def define_yum_test_task
    namespace :yum do
      desc "Test RPM packages"
      task :test do
        repositories_dir = "yum/repositories"
        unless @packages.empty?
          rm_rf(repositories_dir)
          @packages.each do |package|
            package_repositories = "#{package}/yum/repositories"
            next unless File.exist?(package_repositories)
            sh("rsync", "-av", "#{package_repositories}/", repositories_dir)
          end
        end
        rpm_sign(repositories_dir)
        base_dir = "nonexistent"
        yum_update(base_dir, repositories_dir)
        yum_test_targets.each do |target|
          verify(target)
        end
      end
    end
  end
end
