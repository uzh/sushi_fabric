#!/usr/bin/env ruby
# encoding: utf-8
# Version = '20190425-142129'

require 'csv'
require 'fileutils'
require 'yaml'
require 'drb/drb'
gem 'rails'
require 'rails/all'
require 'google-analytics-rails'

module SushiFabric
  class Application < Rails::Application
    # default parameters
    default_root = Rails.root||Dir.pwd
    config.workflow_manager = 'druby://localhost:12345'
    config.gstore_dir = File.join(default_root, 'public/gstore/projects')
    config.sushi_app_dir = default_root
    config.scratch_dir = '/tmp/scratch'
    config.module_source = nil
    config.course_mode = nil
  end

  # load custmized parameters if there is
  mode = ENV['RAILS_ENV']||'production'
  config_file = File.join('./config/environments', mode)
  if File.exist?(config_file + '.rb')
    require config_file
  else
    FileUtils.mkdir_p File.dirname(config_file)
    open(config_file+'.rb', "w") do |out|
      default_root = Rails.root||Dir.pwd
      out.print <<-EOF
module SushiFabric
  class Application < Rails::Application
    # default parameters
    config.workflow_manager = 'druby://localhost:12345'
    config.gstore_dir = File.join(#{default_root}, 'public/gstore/projects')
    config.sushi_app_dir = #{default_root}
    config.scratch_dir = '/tmp/scratch'
    config.module_source = nil
    config.course_mode = nil
  end
end
      EOF
    end
  end

  config = SushiFabric::Application.config
  WORKFLOW_MANAGER = config.workflow_manager
  GSTORE_DIR = config.gstore_dir
  SUSHI_APP_DIR = config.sushi_app_dir
  SCRATCH_DIR = config.scratch_dir
  MODULE_SOURCE = config.module_source
  
  unless File.exist?(GSTORE_DIR)
    FileUtils.mkdir_p GSTORE_DIR
  end

  # check if there is a sqlite3 database of Ruby on Rails
  if defined?(::Project)
    NO_ROR = false
  elsif File.exist?(File.join(SUSHI_APP_DIR, "app/models")) and
    database_yml = File.join(SUSHI_APP_DIR, "config/database.yml") and
    File.exist?(database_yml)

    NO_ROR = false
    
    database_config = YAML.load(File.read(database_yml))
    db = database_config["production"]
    ActiveRecord::Base.establish_connection(
                :adapter => db["adapter"],
                :database => db["database"],
                :username => db["username"],
                :password => db["password"]
            )
    require "#{SUSHI_APP_DIR}/app/models/project"
    require "#{SUSHI_APP_DIR}/app/models/data_set"
    require "#{SUSHI_APP_DIR}/app/models/sample"
    require "#{SUSHI_APP_DIR}/app/models/job"
  else
    NO_ROR = true
  end

class ::Array
  def to_h
    Hash[*flatten]
  end
end
class ::Hash
  attr_reader :defaults
  alias :set :[]=
  alias :get :[]
  def []=(k1,k2,v=nil)
    if v
      @desc ||= {}
      @desc.set([k1,k2].join('_'),v)
    else
      @defaults ||= {}
      if !@defaults[k1] and k2
        if k2.instance_of?(Array)
          @defaults.set(k1,k2.first)
        elsif k2.instance_of?(Hash) and k2.first
          @defaults.set(k1,k2.first.last)
        else
          @defaults.set(k1,k2)
        end
      end
      set(k1,k2)
    end
  end
  def default_value(k,v=nil)
    if v
      @defaults[k] = v
    else
      @defaults[k]
    end
  end
  def data_type(k)
    if @defaults
      @defaults[k].class
    else
      v = get(k)
      if v.instance_of?(Array)
        v.first.class
      elsif v.instance_of?(Hash)
        v.values.first.class
      else
        v.class
      end
    end
  end
  def data_types
    Hash[@defaults.map{|k,v| [k, v.class]}]
  end
  def [](k1, k2=nil)
    if k2
      if @desc
        @desc.get([k1,k2].join('_'))
      else
        nil
      end
    else
      get(k1)
    end
  end
end
class ::String
  def tag?(tag)
    scan(/\[(.*)\]/).flatten.join =~ /#{tag}/
  end
end
def save_data_set(data_set_arr, headers, rows, user=nil)
  data_set_hash = Hash[*data_set_arr]
  unless project = Project.find_by_number(data_set_hash['ProjectNumber'].to_i)
    project = Project.new
    project.number = data_set_hash['ProjectNumber'].to_i
    project.save
  end
  if project = Project.find_by_number(data_set_hash['ProjectNumber'].to_i)
    data_set = DataSet.new
    if user
      data_set.user = user
    end
    data_set.name = data_set_hash['DataSetName']
    data_set.project = project
    if parent_id = data_set_hash['ParentID'] and parent_data_set = DataSet.find_by_id(parent_id.to_i)
      data_set.data_set = parent_data_set
    end
    if comment = data_set_hash['Comment'] and !comment.to_s.empty?
      data_set.comment = comment
    end

    sample_hash = {}
    rows.each do |row|
      headers.each_with_index do |header, i|
       sample_hash[header]=row[i]
      end
      sample = Sample.new
      sample.key_value = sample_hash.to_s
      sample.save unless sample.saved?
      data_set.samples << sample
    end

    data_set.md5 = data_set.md5hexdigest
    unless data_set.saved?
      project.data_sets << data_set
      parent_data_set.data_sets << data_set if parent_data_set
      data_set.save
      if user
        user.data_sets << data_set
        user.save
      end
    else
      headers[0] = DataSet.find_by_md5(data_set.md5)
    end
    data_set.id
  end
end
#module_function :save_data_set

class SushiApp
  attr_reader :params
  attr_reader :job_ids
  attr_reader :next_dataset_id
  attr_reader :required_columns
  attr_reader :required_params
  attr_reader :dataset_hash
  attr_reader :analysis_category
  attr_reader :description
  attr_reader :name
  attr_reader :modules
  attr_accessor :dataset_tsv_file
  attr_accessor :parameterset_tsv_file
  attr_accessor :dataset_sushi_id
  attr_accessor :data_set
  attr_accessor :project
  attr_accessor :user
  attr_accessor :next_dataset_name
  attr_accessor :dataset_name
  attr_accessor :next_dataset_comment
  attr_accessor :workflow_manager
  attr_accessor :current_user
  attr_accessor :logger
  attr_accessor :off_bfabric_registration
  attr_accessor :mango_run_name
  attr_accessor :input_dataset_bfabric_application_number
  attr_accessor :next_dataset_bfabric_application_number
  def initialize
    @gstore_dir = GSTORE_DIR
    @project = nil
    @name = nil
    @params = {}
    @params['cores'] = nil
    @params['ram'] = nil
    @params['scratch'] = nil
    @params['node'] = ''
    @params['process_mode'] = 'SAMPLE'
    @params['samples'] = ''
    @job_ids = []
    @required_columns = []
    @module_source = MODULE_SOURCE
    @modules = []
    #@workflow_manager = workflow_manager_instance||DRbObject.new_with_uri(WORKFLOW_MANAGER)
  end
  def set_input_dataset
    if @dataset_tsv_file
      dataset_tsv = CSV.readlines(@dataset_tsv_file, {:headers=>true, :col_sep=>"\t"})
      @dataset_hash = []
      @dataset = []
      dataset_tsv.each do |row|
        @dataset_hash << row.to_hash
        @dataset << row.to_hash
      end

      # save in sushi db unless it is saved in sushi db
      data_set_arr = []
      headers = []
      rows = []
      dataset_name = if @dataset_name
                       @dataset_name
                     else
                       File.basename(@dataset_tsv_file).gsub(/.tsv/, '')
                     end
      data_set_arr = {'DataSetName'=>dataset_name, 'ProjectNumber'=>@project.gsub(/p/,'')}
      csv = CSV.readlines(@dataset_tsv_file, :col_sep=>"\t")
      csv.each do |row|
        if headers.empty?
          headers = row
        else
          rows << row
        end
      end
      unless NO_ROR
        @current_user ||= nil
        if @dataset_sushi_id = save_data_set(data_set_arr.to_a.flatten, headers, rows, @current_user)
          unless @off_bfabric_registration
            if dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
              dataset.register_bfabric(bfabric_application_number: @input_dataset_bfabric_application_number)
            end
          end
        elsif data_set = headers[0] and data_set.instance_of?(DataSet)
          @dataset_sushi_id = data_set.id
        end
      end
    elsif @dataset_sushi_id
      @dataset_hash = []
      @dataset = []
      if dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
        dataset.samples.each do |sample|
          @dataset_hash << sample.to_hash
          @dataset << sample.to_hash
        end
      end
    end
    @dataset_hash
  end
  def get_columns_with_tag(tag)
    #@factor_cols = @dataset_hash.first.keys.select{|header| header =~ /\[#{tag}\]/}.map{|header| header.gsub(/\[.+\]/,'').strip}
    @dataset_hash.map{|row| 
      Hash[*row.select{|k,v| k=~/\[#{tag}\]/}.map{|k,v| [k.gsub(/\[.+\]/,'').strip,v]}.flatten]
    }
  end
  def set_default_parameters
    # this should be overwritten in a subclass
  end
  def dataset_has_column?(colname)
    flag = false
    if @dataset_hash
      @dataset_hash.map{|sample| 
        sample.each do |key, value|
          if key =~ /#{colname}/
            flag = true
          end
        end
        break
      }
    end
    flag
  end

  def set_output_files
    if @params['process_mode'] == 'SAMPLE'
      @dataset = {}
    end
    next_dataset.keys.select{|header| header.tag?('File')}.each do |header|
      @output_files ||= []
      @output_files << header
    end
    if @output_files
      @output_files = @output_files.uniq
    end
  end
  def check_required_columns
    if @dataset_hash and @required_columns and (@required_columns-@dataset_hash.map{|row| row.keys}.flatten.uniq.map{|colname| colname.gsub(/\[.+\]/,'').strip}).empty?
      true
    else
      false
    end
  end
  def check_application_parameters
    if @required_params and (@required_params - @params.keys).empty?
      @output_params = @params.clone
    end
  end
  def set_user_parameters
    # this should be done in an instance of applicaiton subclass
    if @parameterset_tsv_file
      parameterset_tsv = CSV.readlines(@parameterset_tsv_file, :col_sep=>"\t")
      headers = []
      parameterset_tsv.each do |row|
        header, value = row
        headers << header
        @params[header] = if @params.data_type(header) == String or value == nil
                            value
                          else
                            eval(value)
                          end
      end
      (@params.keys - headers).each do |key|
        @params[key] = @params.default_value(key)
      end
    end
    @params
  end
  def set_dir_paths
    ## sushi figures out where to put the resulting dataset
    unless @name and @project
      raise "should set #name and #project"
    end
    @name.gsub!(/\s/,'_')
    @result_dir_base = if @next_dataset_name
                        [@next_dataset_name, Time.now.strftime("%Y-%m-%d--%H-%M-%S")].join("_")
                      else
                        [@name, @dataset_sushi_id.to_s, Time.now.strftime("%Y-%m-%d--%H-%M-%S")].join("_")
                      end
    @result_dir = File.join(@project, @result_dir_base)
    @scratch_result_dir = File.join(SCRATCH_DIR, @result_dir_base)
    @job_script_dir = File.join(@scratch_result_dir, 'scripts')
    @gstore_result_dir = File.join(@gstore_dir, @result_dir)
    @gstore_script_dir = File.join(@gstore_result_dir, 'scripts')
    @gstore_project_dir = File.join(@gstore_dir, @project)
    set_file_paths
  end
  def prepare_result_dir
    FileUtils.mkdir_p(@scratch_result_dir)
    FileUtils.mkdir_p(@job_script_dir)
  end
  def check_latest_module_version(mod)
    command_out =  %x[ bash -lc "source #{@module_source}; module whatis #{mod} 2>&1" ]
    latest_mod = command_out.split.first
    latest_mod = nil if latest_mod == "Failed"
    latest_mod
  end
  def job_header
    @scratch_dir = if @params['process_mode'] == 'SAMPLE'
                     @scratch_result_dir + "_" + @dataset['Name'] + '_temp$$'
                   else
                     @scratch_result_dir + '_temp$$'
                   end
    hold_jid_option = if @dataset_sushi_id and parent_data_set = DataSet.find_by_id(@dataset_sushi_id.to_i) and !parent_data_set.jobs.empty?
                                parent_data_set_job_ids = parent_data_set.jobs.map{|job| job.submit_job_id}.join(",")
                                "#\$ -hold_jid #{parent_data_set_job_ids}"
                              else
                                ''
                              end
    module_src_command = if @module_source and @modules and !@modules.empty?
                       "source #{@module_source}"
                     else
                       ""
                     end
    module_add_commands = if @modules and !@modules.empty?
                            modules_with_version = @modules.map{|mod| check_latest_module_version(mod)}
                            modules_with_version.compact!
                            "module add #{modules_with_version.join(' ')}"
                            #"module add #{@modules.join(' ')}"
                          else
                            ""
                          end
    @out.print <<-EOF
#!/bin/bash
#{hold_jid_option}
set -e
set -o pipefail
umask 0002

#### SET THE STAGE
SCRATCH_DIR=#{@scratch_dir}
GSTORE_DIR=#{@gstore_dir}
INPUT_DATASET=#{@input_dataset_tsv_path}
echo "Job runs on `hostname`"
echo "at $SCRATCH_DIR"
mkdir $SCRATCH_DIR || exit 1
cd $SCRATCH_DIR || exit 1
#{module_src_command}
#{module_add_commands}

    EOF
  end
  def job_footer
    @out.print "#### JOB IS DONE WE PUT THINGS IN PLACE AND CLEAN AUP\n"
    if @output_files
      @output_files.map{|header| next_dataset[header]}.each do |file|
        # in actual case, to save under /srv/gstore/
        src_file = File.basename(file)
        dest_dir = File.dirname(File.join(@gstore_dir, file))
        @out.print copy_commands(src_file, dest_dir).join("\n"), "\n"
      end
    end
    @out.print <<-EOF
cd #{SCRATCH_DIR}
rm -rf #{@scratch_dir} || exit 1

    EOF

  end
  def job_main
    @out.print "#### NOW THE ACTUAL JOBS STARTS\n"
    @out.print commands, "\n\n"
  end
  def next_dataset
    # this should be overwritten in a subclass
  end
  def commands
    # this should be overwritten in a subclass
  end
  def submit_command(job_script)
    gsub_options = []
    gsub_options << "-c #{@params['cores']}" unless @params['cores'].to_s.empty?
    gsub_options << "-n #{@params['node']}" unless @params['node'].to_s.empty?
    gsub_options << "-r #{@params['ram']}" unless @params['ram'].to_s.empty?
    gsub_options << "-s #{@params['scratch']}" unless @params['scratch'].to_s.empty?
    command = "wfm_monitoring --server #{WORKFLOW_MANAGER} --user #{@user} --project #{@project.gsub(/p/,'')} --logdir #{@gstore_script_dir} #{job_script} #{gsub_options.join(' ')}"
    puts "submit: #{command}"

    project_number = @project.gsub(/p/, '')
    @workflow_manager||=DRbObject.new_with_uri(WORKFLOW_MANAGER)
    script_content = File.read(job_script)
    job_id = 0
    begin
      job_id = @workflow_manager.start_monitoring(job_script, @user, 0, script_content, project_number, gsub_options.join(' '), @gstore_script_dir)
      #job_id = @workflow_manager.start_monitoring2(job_script, script_content, @user, project_number, gsub_options.join(' '), @gstore_script_dir)
    rescue => e
      time = Time.now.strftime("[%Y.%m.%d %H:%M:%S]")
      @logger.error("*"*50)
      @logger.error("submit_command error #{time}")
      @logger.error("error: #{e}")
      @logger.error("job_script: #{job_script}, @user: #{@user}, script_content: #{script_content.class} #{script_content.to_s.length} chrs, project_number: #{project_number}, gsub_options: #{gsub_options}, job_id: #{job_id}")
      @logger.error("*"*50)
    end
    job_id
  end
  def submit(job_script, mock=false)
    begin
      job_id = unless mock
                 i = submit_command(job_script)
                 i.to_i
               else
                 #Time.now.to_f.to_s.gsub('.', '')
                 1234
               end
      unless job_id.to_i > 1
        @logger.error("#"*50)
        time = Time.now.strftime("[%Y.%m.%d %H:%M:%S]")
        @logger.error("error happened in job submitting, but maybe fine. #{time}")
        @logger.error("#"*50)
        job_id = nil
      end
    rescue
      @logger.error("@"*50)
      time = Time.now.strftime("[%Y.%m.%d %H:%M:%S]")
      @logger.error("error happened in job submitting, but maybe fine. #{time}")
      @logger.error("@"*50)
      job_id = nil
    end
    job_id
  end
  def preprocess
    # this should be overwritten in a subclass
  end
  def set_file_paths
    @parameter_file = 'parameters.tsv'
    @input_dataset_file = 'input_dataset.tsv'
    @next_dataset_file = 'dataset.tsv'
    @input_dataset_tsv_path = File.join(@gstore_result_dir, @input_dataset_file)
    @parameters_tsv_path = File.join(@gstore_result_dir, @parameter_file)
    @next_dataset_tsv_path = File.join(@gstore_result_dir, @next_dataset_file)
  end
  def save_parameters_as_tsv
    file_path = File.join(@scratch_result_dir, @parameter_file)
    CSV.open(file_path, 'w', :col_sep=>"\t") do |out|
      out << ["sushi_app", self.class.name]
      @output_params.each do |key, value|
        out << [key, value]
      end
    end
    file_path
  end
  def save_input_dataset_as_tsv
    file_path = File.join(@scratch_result_dir, @input_dataset_file)
    CSV.open(file_path, 'w', :col_sep=>"\t") do |out|
      headers = @dataset_hash.map{|row| row.keys}.flatten.uniq
      out << headers
      @dataset_hash.each do |row|
        out << headers.map{|header| 
          val = row[header]
          val.to_s.empty? ? nil:val
        }
      end
    end
    file_path
  end
  def save_next_dataset_as_tsv
    headers = @result_dataset.map{|row| row.keys}.flatten.uniq
    file_path = File.join(@scratch_result_dir, @next_dataset_file)
    CSV.open(file_path, 'w', :col_sep=>"\t") do |out|
      out << headers
      @result_dataset.each do |row_hash|
        out << headers.map{|header| 
          val = row_hash[header]
          val.to_s.empty? ? nil:val
        }
      end
    end
    file_path
  end
  def copy_commands(org_dir, dest_parent_dir, now=nil)
    @workflow_manager||=DRbObject.new_with_uri(WORKFLOW_MANAGER)
    com = ''
    cnt_retry = 0
    begin
      com = @workflow_manager.copy_commands(org_dir, dest_parent_dir, now)
    rescue => e
      time = Time.now.strftime("[%Y.%m.%d %H:%M:%S]")
      @logger.error("*"*50)
      @logger.error("copy_command error #{time}")
      @logger.error("error: #{e}")
      @logger.error("org_dir: #{org_dir}, dest_parent_dir: #{dest_parent_dir}, now: #{now}")
      @logger.error("*"*50)
      sleep 1
      cnt_retry += 1
      retry if cnt_retry < 3
    end
    com
  end
  def copy_inputdataset_parameter_jobscripts
    org = @scratch_result_dir
    dest = @gstore_project_dir
    copy_commands(org, dest, 'now').each do |command|
      puts `which python`
      puts command
      unless system command
        raise "fails in copying input_dataset, parameters and jobscript files from /scratch to /gstore"
      end
    end
    #sleep 1
  end
  def copy_nextdataset
    org = @next_dataset_tsv_path
    dest = File.join(@gstore_project_dir, @result_dir_base)
    copy_commands(org, dest, 'now').each do |command|
      puts `which python`
      puts command
      unless system command
        raise "fails in copying next_dataset files from /scratch to /gstore"
      end
    end
    sleep 1
    command = "rm -rf #{@scratch_result_dir}"
    `#{command}`
  end
  def cluster_nodes
    @workflow_manager||=DRbObject.new_with_uri(WORKFLOW_MANAGER)
    @workflow_manager.cluster_nodes
  end
  def default_node
    @workflow_manager||=DRbObject.new_with_uri(WORKFLOW_MANAGER)
    @workflow_manager.default_node
  end

  def make_job_script(append = false)
    @out = if append
             open(@job_script, 'a')
           else
             open(@job_script, 'w')
           end
    job_header
    job_main
    job_footer
    @out.close
  end
  def sample_mode
    selected_samples = Hash[*@params['samples'].split(',').map{|sample_name| [sample_name, true]}.flatten]
    @dataset_hash.each do |row|
      @dataset = Hash[*row.map{|key,value| [key.gsub(/\[.+\]/,'').strip, value]}.flatten]
      if selected_samples[@dataset['Name']]
        ## WRITE THE JOB SCRIPT
        sample_name = @dataset['Name']||@dataset.first
        @job_script = if @dataset_sushi_id and dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
                        File.join(@job_script_dir, @analysis_category + '_' + sample_name) + '_' + dataset.name.gsub(/\s+/,'_') + '.sh'
                      else
                        File.join(@job_script_dir, @analysis_category + '_' + sample_name) + '.sh'
                      end
        make_job_script
        @job_scripts << @job_script
        @result_dataset << next_dataset
      end
    end
  end
  def dataset_mode
    @dataset = @dataset_hash # for a case of @dataset is used in def next_datast in SUSHIApp
    @job_script = if @dataset_sushi_id and dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
                    File.join(@job_script_dir, @analysis_category + '_' + dataset.name.gsub(/[\s+,\/]/,'_') + '.sh')
                  else 
                    File.join(@job_script_dir, @analysis_category + '_' + 'job_script.sh')
                  end
    make_job_script
    @job_scripts << @job_script
    @result_dataset << next_dataset
  end
  def batch_mode
    @job_script = if @dataset_sushi_id and dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
                    File.join(@job_script_dir, dataset.name.gsub(/\s+/,'_') + '.sh')
                  else 
                    File.join(@job_script_dir, 'job_script.sh')
                  end
    @dataset_hash.each do |row|
      @dataset = Hash[*row.map{|key,value| [key.gsub(/\[.+\]/,'').strip, value]}.flatten]
      make_job_script('append')
      @result_dataset << next_dataset
    end
    @job_scripts << @job_script
  end
  def save_data_set(data_set_arr, headers, rows, user=nil, child=nil)
    data_set_hash = Hash[*data_set_arr]
    unless project = Project.find_by_number(data_set_hash['ProjectNumber'].to_i)
      project = Project.new
      project.number = data_set_hash['ProjectNumber'].to_i
      project.save
    end
    if project = Project.find_by_number(data_set_hash['ProjectNumber'].to_i)
      data_set = DataSet.new
      if user
        data_set.user = user
      end
      data_set.name = data_set_hash['DataSetName']
      data_set.project = project
      if parent_id = data_set_hash['ParentID'] and parent_data_set = DataSet.find_by_id(parent_id.to_i)
        data_set.data_set = parent_data_set
        data_set.sushi_app_name = self.class.name
      end
      if comment = data_set_hash['Comment'] and !comment.to_s.empty?
        data_set.comment = comment
      end
      if @mango_run_name
        data_set.run_name_order_id = @mango_run_name
      end

      sample_hash = {}
      rows.each do |row|
        headers.each_with_index do |header, i|
         sample_hash[header]=row[i]
        end
        sample = Sample.new
        sample.key_value = sample_hash.to_s
        sample.save unless sample.saved?
        data_set.samples << sample
      end

      if child
        data_set.child = true
      end

      data_set.md5 = data_set.md5hexdigest
      unless data_set.saved?
        project.data_sets << data_set
        parent_data_set.data_sets << data_set if parent_data_set
        data_set.save
        if user
          user.data_sets << data_set
          user.save
        end
      else
        headers[0] = DataSet.find_by_md5(data_set.md5)
      end
      data_set.id
    end
  end
  def main(mock=false)
    ## sushi writes creates the job scripts and builds the result data set that is to be generated
    @result_dataset = []
    @job_scripts = []
    if @params['process_mode'] == 'SAMPLE'
      sample_mode
    elsif @params['process_mode'] == 'DATASET'
      dataset_mode
    elsif @params['process_mode'] == 'BATCH'
      batch_mode
    else 
      #stop
      warn "the process mode (#{@params['process_mode']}) is not defined"
      raise "stop job submitting"
    end
    if mock
      make_dummy_files
    end
    copy_inputdataset_parameter_jobscripts

    # job submittion
    gstore_job_script_paths = []
    @job_scripts.each_with_index do |job_script, i|
      if job_id = submit(job_script, mock)
        @job_ids << job_id
        print "Submit job #{File.basename(job_script)} job_id=#{job_id}"
        gstore_job_script_paths << File.join(@gstore_script_dir, File.basename(job_script))
      end
    end

    puts
    print 'job scripts: '
    p @job_scripts
    print 'result dataset: '
    p @result_dataset

    # copy application data to gstore 
    @next_dataset_tsv_path = save_next_dataset_as_tsv

    if !@job_ids.empty? and @dataset_sushi_id and dataset = DataSet.find_by_id(@dataset_sushi_id.to_i)
      data_set_arr = []
      headers = []
      rows = []
      next_dataset_name = if name = @next_dataset_name
                            name.to_s
                          else
                            "#{@name.gsub(/\s/,'').gsub(/_/,'')}_#{dataset.id}"
                          end
      data_set_arr = {'DataSetName'=>next_dataset_name, 'ProjectNumber'=>@project.gsub(/p/,''), 'ParentID'=>@dataset_sushi_id, 'Comment'=>@next_dataset_comment.to_s}
      csv = CSV.readlines(@next_dataset_tsv_path, :col_sep=>"\t")
      csv.each do |row|
        if headers.empty?
          headers = row
        else
          rows << row
        end
      end
      unless NO_ROR
        @current_user ||= nil
        @next_dataset_id = save_data_set(data_set_arr.to_a.flatten, headers, rows, @current_user, @child)

        unless @off_bfabric_registration
          if next_dataset = DataSet.find_by_id(@next_dataset_id)
            next_dataset.register_bfabric(bfabric_application_number: @next_dataset_bfabric_application_number)
          end
        end

        # save job and dataset relation in Sushi DB
        job_ids.each_with_index do |job_id, i|
          new_job = Job.new
          new_job.submit_job_id = job_id.to_i
          new_job.script_path = gstore_job_script_paths[i]
          new_job.next_dataset_id = @next_dataset_id
          new_job.save
          new_job.data_set.jobs << new_job
          new_job.data_set.save
        end

      end
    end
    copy_nextdataset
  end
  def run
    test_run

    ## the user presses RUN
    prepare_result_dir

    ## copy application data to gstore 
    save_parameters_as_tsv
    save_input_dataset_as_tsv

    if SushiFabric::Application.config.fgcz?
      # this causes sqlite3 IO error in Mac OSX (Yosemite)
      pid = Process.fork do
        Process.fork do
          main
        end # grand-child process
      end # child process
      Process.waitpid pid
    else
      main
    end
  end
  def make_dummy_files
    dummy_files_header = []
    headers = @result_dataset.map{|row| row.keys}.flatten.uniq
    headers.select{|header| header.tag?('File')||header.tag?('Link')}.each do |header|
      dummy_files_header << header
    end
    dummy_files_ = []
    @result_dataset.each do |row|
      dummy_files_.concat(dummy_files_header.map{|header| row[header]})
    end
    dummy_files = []
    dummy_files_.each do |file|
      dummy_files << file.gsub(@result_dir, '')
    end
    dummy_files.uniq!

    dirs = []
    dummy_files.permutation(2).each do |a,b|
      if a.include?(b) and b !~ /\./
        dirs << b
      end
    end
    dirs.each do |dir|
      dummy_files.delete(dir)
    end
    dirs.each do |dir|
      command = "mkdir -p #{File.join(@scratch_result_dir, dir)}"
      puts command
      `#{command}`
    end
    dummy_files.each do |file|
      command = if file =~ /.html/
                  "echo 'Hello, SUSHI world!' > #{File.join(@scratch_result_dir, file)}"
                else
                  "touch #{File.join(@scratch_result_dir, file)}"
                end
      puts command
      `#{command}`
    end
  end
  def mock_run
    test_run
    prepare_result_dir
    save_parameters_as_tsv
    save_input_dataset_as_tsv
    main(true)
  end
  def test_run
    set_input_dataset
    set_dir_paths
    preprocess
    set_output_files
    set_user_parameters

    failures = 0
    err_msgs = []
    print 'check project name: '
    unless @project
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: project number is required but not found. you should set it in usecase."
      err_msg << "\tex.)"
      err_msg << "\tapp = #{self.class}.new"
      err_msg << "\tapp.project = 'p1001'"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:\n\t@project=#{@project}"
    end

    print 'check user name: '
    unless @user
      err_msg = []
      err_msg << "\e[31mWARNING\e[0m: user number is ought to be added but not found. you should set it in usecase. Default will be 'sushi lover'"
      err_msg << "\tex.)"
      err_msg << "\tapp = #{self.class}.new"
      err_msg << "\tapp.user = 'masa'"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
    else
      puts "\e[32mPASSED\e[0m:\n\t@user=#{@user}"
    end

    print 'check application name: '
    if @name.to_s.empty?
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: application name is required but not found. you should set it in application class."
      err_msg << "\tex.)"
      err_msg << "\tclass #{self.class}"
      err_msg << "\t def initialize"
      err_msg << "\t  @name = '#{self.class}'"
      err_msg << "\t end"
      err_msg << "\tend"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:\n\t@name=#{@name}"
    end

    print 'check analysis_category: '
    if @analysis_category.to_s.empty?
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: analysis_category is required but not found. you should set it in application class."
      err_msg << "\tex.)"
      err_msg << "\tclass #{self.class}"
      err_msg << "\t def initialize"
      err_msg << "\t  @analysis_category = 'Mapping'"
      err_msg << "\t end"
      err_msg << "\tend"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:\n\t@analysis_category=#{@analysis_category}"
    end

    print 'check dataset: '
    if !@dataset_hash or @dataset_hash.empty?
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: dataset is not found. you should set it by using #{self.class}#dataset_sushi_id or #{self.class}#dataset_tsv_file properties"
      err_msg << "\tex.)"
      err_msg << "\tusecase = #{self.class}.new"
      err_msg << "\tusecase.dataset_tsv_file = \"dataset.tsv\""
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:\n\t@dataset_hash.length = #{@dataset_hash.length}"
    end

    print 'check required columns: '
    unless check_required_columns
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: required_column(s) is not found in dataset. you should set it in application class."
      err_msg << "\tex.)"
      err_msg << "\tdef initialize"
      err_msg << "\t  @required_columns = ['Name', 'Read1']"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:"
    end
    puts "\trequired columns: #{@required_columns}"
    puts "\tdataset  columns: #{@dataset_hash.map{|row| row.keys}.flatten.uniq}" if @dataset_hash

    print 'check required parameters: '
    unless check_application_parameters
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: required_param(s) is not set yet. you should set it in usecase"
      err_msg << "\tmissing params: #{@required_params-@params.keys}" if @required_params
      err_msg << "\tex.)"
      err_msg << "\tusecase = #{self.class}.new"
      if @required_params
        err_msg << "\tusecase.params['#{(@required_params-@params.keys)[0]}'] = parameter"
      else
        err_msg << "\tusecase.params['parameter name'] = default_parameter"
      end
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:"
    end
    puts "\tparameters: #{@params.keys}"
    puts "\trequired  : #{@required_params}"

    print 'check next dataset: '
    if @params['process_mode'] == 'SAMPLE'
      @dataset={}
    end
    unless self.next_dataset
      err_msg = []
      err_msg << "\e[31mFAILURE\e[0m: next dataset is not set yet. you should overwrite SushiApp#next_dataset method in #{self.class}"
      err_msg << "\tnote: the return value should be Hash (key: column title, value: value in a tsv table)"
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
      failures += 1
    else
      puts "\e[32mPASSED\e[0m:"
    end

    print 'check output files: '
    if !@output_files or @output_files.empty?
      err_msg = []
      err_msg << "\e[31mWARNING\e[0m: no output files. you will not get any output files after the job running. you can set @output_files (array) in #{self.class}"
      err_msg << "\tnote: usually it should be define in initialize method"
      err_msg << "\t      the elements of @output_files should be chosen from #{self.class}#next_dataset.keys"
      err_msg << "\t      #{self.class}#next_dataset.keys: #{self.next_dataset.keys}" if self.next_dataset
      puts err_msg.join("\n")
      err_msgs.concat(err_msg)
    else
      puts "\e[32mPASSED\e[0m:"
    end

    print 'check commands: '
    if @params['process_mode'] == 'SAMPLE'
      @dataset_hash.each do |row|
        @dataset = Hash[*row.map{|key,value| [key.gsub(/\[.+\]/,'').strip, value]}.flatten]
        unless com = commands
          err_msg = []
          err_msg << "\e[31mFAILURE\e[0m: any commands is not defined yet. you should overwrite SushiApp#commands method in #{self.class}"
          err_msg << "\tnote: the return value should be String (this will be in the main body of submitted job script)"
          puts err_msg.join("\n")
          err_msgs.concat(err_msg)
          failures += 1
        else
          puts "\e[32mPASSED\e[0m:"
          puts "generated command will be:"
          puts "\t"+com.split(/\n/).join("\n\t")+"\n"
        end
      end
    elsif @params['process_mode'] == 'DATASET'
      unless com = commands
        err_msg = []
        err_msg << "\e[31mFAILURE\e[0m: any commands is not defined yet. you should overwrite SushiApp#commands method in #{self.class}"
        err_msg << "\tnote: the return value should be String (this will be in the main body of submitted job script)"
        puts err_msg.join("\n")
        err_msgs.concat(err_msg)
        failures += 1
      else
        puts "\e[32mPASSED\e[0m:"
        puts "generated command will be:"
        puts "\t"+com.split(/\n/).join("\n\t")+"\n"
      end
    end

    print 'check workflow manager: '
    begin
      @workflow_manager||=DRbObject.new_with_uri(WORKFLOW_MANAGER)
      hello = @workflow_manager.hello
    rescue
    end
    unless hello =~ /hello/
      err_msg = "\e[31mFAILURE\e[0m: workflow_manager does not reply. check if workflow_manager is working"
      puts err_msg
      err_msgs.concat([err_msg])
      failures += 1
    else
      puts "\e[32mPASSED\e[0m: #{WORKFLOW_MANAGER}"
    end

    if failures > 0
      puts
      err_msg = "\e[31mFailures (#{failures})\e[0m: All failures should be solved"
      puts err_msg
      err_msgs.unshift(err_msg)
      raise "\n"+err_msgs.join("\n")+"\n\n"
    else
      puts "All checks \e[32mPASSED\e[0m"
    end
  end
end


end
