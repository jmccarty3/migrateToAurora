#!/usr/bin/env ruby

require 'aws-sdk'
require 'mysql2'
require 'optparse'

def setup_read_replica(client, options)
  resp = client.describe_db_instances({db_instance_identifier: options[:db]})

  if resp.db_instances.size > 1
    raise 'Multiple responses. Aborting'
  end

  if resp.db_instances[0].db_instance_status != "available"
    raise "Source DB not available"
  end

  #Check for an existing database
  begin
    resp = client.describe_db_instances({db_instance_identifier: options[:read_rep]})

    if resp.db_instances.size > 0
      if resp.db_instances[0].read_replica_source_db_instance_identifier == options[:db]
        puts "Read Replica Currently exists. Skipping"
        return Aws::RDS::DBInstance.new(resp.db_instances[0].db_instance_identifier, {client: client})
      else
        raise "Target read replica currently exists but is not configured as a read replica of source db"
      end
    end
  rescue Aws::RDS::Errors::DBInstanceNotFound
  end

  puts "Creating Read Replica %s" % [options[:read_rep]]
  puts options[:read_rep]

  resp = client.create_db_instance_read_replica({
    db_instance_identifier: options[:read_rep],
    source_db_instance_identifier: options[:db],
    })

  replica = Aws::RDS::DBInstance.new(resp.db_instance.db_instance_identifier, {client: client})
  while replica.db_instance_status != "available"
    puts "Waiting for replica to become available"
    sleep 60
    replica = Aws::RDS::DBInstance.new(resp.db_instance.db_instance_identifier, {client: client})
  end

  puts "Replica Created. Current Status %s " % replica.db_instance_status

  replica
end

def configure_read_rep(replica)

  if replica.backup_retention_period > 0
    return
  end
  puts "Configuring read replica"

  replica.modify({
    backup_retention_period: 1,
    apply_immediately: true
    })


  while replica.pending_modified_values.values.any? { |value| !value.nil? }
    puts "Waiting for modifications to be applied"
    sleep 30
  end

  while replica.db_instance_status != "available"
    puts "Waiting for replica to become available after modify"
    sleep 30
  end
end

def stop_replication(mysql)
  resp = mysql.query("CALL mysql.rds_stop_replication")
  mysql.abandon_results!

  puts "Stopped Replication. Pausing to allow current operations to finish"
  sleep 30
  #resp.free
end

def create_snapshot(client,replica)
  puts "Creating Snapshot: %s" % [replica.db_instance_identifier + "-aurora"]
  resp = client.create_db_snapshot({
    db_snapshot_identifier: replica.db_instance_identifier + "-aurora",
    db_instance_identifier: replica.db_instance_identifier
    })

  while client.describe_db_snapshots({db_snapshot_identifier: resp.db_snapshot.db_snapshot_identifier }).db_snapshots[0].status != "available"
    puts "Waiting for Snapshot to become ready"
    sleep 60
  end

  puts "Snapshot created: #{resp.db_snapshot.db_snapshot_identifier}"

  resp.db_snapshot.db_snapshot_identifier
end

def create_aurora(client,options, replica, snapshot)
  aurora_created = false
  aurora = nil
  start = Time.now
  first_run = true
  while aurora_created == false
    begin
      resp = client.describe_db_instances({
        db_instance_identifier: options[:target]})

      if resp.db_instances.size > 0
        if resp.db_instances[0].db_cluster_identifier != options[:target_cluster]
          raise "incorrect cluster for #{options[:target]}. Expected #{options[:target_cluster]}"
        end

        aurora = Aws::RDS::DBInstance.new(options[:target], {client: client})
        aurora_created = true
        puts "Aurora Cluster found"
      end
    rescue Aws::RDS::Errors::DBInstanceNotFound
      #Currently this functionality is not supported by amazon.
      #  puts "Starting Aurora creation at: #{start}"
      #  resp = client.restore_db_cluster_from_snapshot({
      #    db_cluster_identifier: options[:target],
      #    snapshot_identifier: snapshot,
      #    engine: "aurora",
      #    db_subnet_group_name: replica.db_subnet_group.db_subnet_group_name,
      #    vpc_security_group_ids: replica.vpc_security_groups.collect { |x| x.vpc_security_group_id }
      #  })
      if first_run == true
        puts "AWS SDK functionality missing. Please migrate the snapshot #{snapshot} through the web UI to Instance #{options[:target]} in cluster #{options[:target_cluster]}"
        first_run = false
      end
      sleep 60
    end
  end

  while aurora.db_instance_status != "available"
    puts "Waiting for aurora cluster to become available"
    sleep 60
    aurora = Aws::RDS::DBInstance.new(options[:target], {client: client})
  end

  if !options[:group].nil?
    puts "Setting Parameter group to #{options[:group]}. The triggering reboot"
    aurora.modify({
     db_parameter_group_name: options[:group]
    })

    client.reboot_db_instance({
     db_instance_identifier: options[:target]})

     sleep 30
     aurora = Aws::RDS::DBInstance.new(options[:target], {client: client})

     while aurora.db_instance_status != "available"
       puts "Waiting for aurora cluster to become available"
       sleep 60
       aurora = Aws::RDS::DBInstance.new(options[:target], {client: client})
     end
  end
  endTime=Time.now

  puts "Aurora Cluster Created. Current Status %s " % replica.db_instance_status
  puts "Creation Finished at: #{endTime}. Total of #{endTime-start} seconds"
  aurora
end

def sync_replicas(replicaMysql, auroraMySql)
  bin_file =''
  bin_location = 0
  #Get Master Status for Replica
  replicaMysql.query("SHOW MASTER STATUS").each do |row|
    bin_file = row['File']
    bin_location = row['Position'].to_i
  end

  replicaMysql.abandon_results!

  puts "Replicating to Aurora cluster using Bin File #{bin_file} at location #{bin_location}"
  # Set external master for Auroa
  auroraMySql.query("CALL mysql.rds_set_external_master('#{replicaMysql.query_options[:host]}', 3306,
  '#{replicaMysql.query_options[:username]}', '#{replicaMysql.query_options[:password]}', '#{bin_file}', #{bin_location}, 0)")
  auroraMySql.abandon_results!
  # Start Replication
  auroraMySql.query("CALL mysql.rds_start_replication")
  auroraMySql.abandon_results!
  #Wait for lag to be 0 in Aurora

  synced = false

  while synced == false
    auroraMySql.query("SHOW SLAVE STATUS").each do |row|
      if row["Seconds_Behind_Master"].nil?
        puts "Unknown time behind master"
      else
        puts "Aurora #{row["Seconds_Behind_Master"]} seconds behind master"
        if row["Seconds_Behind_Master"] == 0
          synced = true
        end
      end

      if !row["Last_Error"].empty?
        raise "Error in Aurora Replication: #{row["Last_Error"]}"
      end
    end
    auroraMySql.abandon_results!
    sleep 60
  end
  # Start Replication on Rep
  puts "Aurora Synced. Enabling Main repllica replication"
  replicaMysql.query("CALL mysql.rds_start_replication")
  auroraMySql.abandon_results!
end

begin
  STDOUT.sync = true

  options = {}
  OptionParser.new do |opts|
    opts.banner = "Usage: migrate-db-to-aurora.rb [options]"
    opts.on('-d','--database NAME', 'Database to convert') do |d|
      options[:db] = d
    end
    opts.on('-u', '--user USERNAME', 'User to perform operations with') do |u|
      options[:user] = u
    end
    opts.on('-p', '--pass PASSWORD', 'Password to perform operations with') do |p|
      options[:pass] = p
    end
    opts.on('-t', '--target TARGET', 'Target Database ID') do |t|
      options[:target] = t
    end
    opts.on('-s', '--stage STAGE', 'Stage to resume migration from [1-4]') do |s|
      options[:stage] = s.to_i
    end
    opts.on('-g', '--group GROUP', 'Parameter group to apply to aurora instance') do |g|
      options[:group] = g
    end

  end.parse!

  raise OptionParser::MissingArgument if options[:db].nil?
  raise OptionParser::MissingArgument if options[:user].nil?
  raise OptionParser::MissingArgument if options[:pass].nil?

  if options[:target].nil?
    options[:target] = options[:db] + "-migrated"
  end

  options[:target_cluster] = options[:target] + "-cluster"

  if options[:stage].nil?
    options[:stage] = 1
  end

  puts "
  Migrating Database: %s to %s
  Using User: %s
  Starting at Stage %i
  " % [options[:db], options[:target], options[:user], options[:stage]]

  options[:read_rep] = options[:db] + "-readRep"

  start_time = Time.now
  client=Aws::RDS::Client.new(region: "us-east-1")

  puts options
  #Stage 1 Replication Setup
  stage1Time_start = Time.now
  #Create read replica of target DB
  replica = setup_read_replica(client, options)
  #Modify replica to have a backup-retention-period of 1 day. Apply immediatly
  configure_read_rep(replica)


  #Stage 2 Create Aurora
  stage2Time_start = Time.now
  replicaMysql = Mysql2::Client.new(:host => replica.endpoint.address,:username => options[:user], :password => options[:pass],
    :reconnect => true)
  #Stop Replication of client
  stop_replication(replicaMysql)

  replicaMysql.close

  #Create snapshot of replica
  snapshot = create_snapshot(client, replica)

  #Create restore-db-cluster-from-snapshot with auroa
  aurora = create_aurora(client, options, replica, snapshot)


  #Stage 3 Begin Replication
  stage3Time_start = Time.now
  replicaMysql = Mysql2::Client.new(:host => replica.endpoint.address,:username => options[:user], :password => options[:pass],
    :reconnect => true)
  auroraMySql = Mysql2::Client.new(:host => aurora.endpoint.address, :username => options[:user], :password => options[:pass],
    :reconnect => true)
  sync_replicas(replicaMysql, auroraMySql)

  end_time = Time.now

  puts "Migration Finished. Elapsed time: #{end_time - start_time} seconds"
rescue Exception => msg
  puts "Error! #{msg.inspect}"
  puts msg
  puts msg.backtrace.join("\n")
end
