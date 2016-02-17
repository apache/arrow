#!/usr/bin/perl
# Copyright 2014 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#######################################################################
# This script will convert a stack trace with addresses:
#     @           0x5fb015 kudu::master::Master::Init()
#     @           0x5c2d38 kudu::master::MiniMaster::StartOnPorts()
#     @           0x5c31fa kudu::master::MiniMaster::Start()
#     @           0x58270a kudu::MiniCluster::Start()
#     @           0x57dc71 kudu::CreateTableStressTest::SetUp()
# To one with line numbers:
#     @           0x5fb015 kudu::master::Master::Init() at /home/mpercy/src/kudu/src/master/master.cc:54
#     @           0x5c2d38 kudu::master::MiniMaster::StartOnPorts() at /home/mpercy/src/kudu/src/master/mini_master.cc:52
#     @           0x5c31fa kudu::master::MiniMaster::Start() at /home/mpercy/src/kudu/src/master/mini_master.cc:33
#     @           0x58270a kudu::MiniCluster::Start() at /home/mpercy/src/kudu/src/integration-tests/mini_cluster.cc:48
#     @           0x57dc71 kudu::CreateTableStressTest::SetUp() at /home/mpercy/src/kudu/src/integration-tests/create-table-stress-test.cc:61
#
# If the script detects that the output is not symbolized, it will also attempt
# to determine the function names, i.e. it will convert:
#     @           0x5fb015
#     @           0x5c2d38
#     @           0x5c31fa
# To:
#     @           0x5fb015 kudu::master::Master::Init() at /home/mpercy/src/kudu/src/master/master.cc:54
#     @           0x5c2d38 kudu::master::MiniMaster::StartOnPorts() at /home/mpercy/src/kudu/src/master/mini_master.cc:52
#     @           0x5c31fa kudu::master::MiniMaster::Start() at /home/mpercy/src/kudu/src/master/mini_master.cc:33
#######################################################################
use strict;
use warnings;

if (!@ARGV) {
  die <<EOF
Usage: $0 executable [stack-trace-file]

This script will read addresses from a file containing stack traces and
will convert the addresses that conform to the pattern " @ 0x123456" to line
numbers by calling addr2line on the provided executable.
If no stack-trace-file is specified, it will take input from stdin.
EOF
}

# el6 and other older systems don't support the -p flag,
# so we do our own "pretty" parsing.
sub parse_addr2line_output($$) {
  defined(my $output = shift) or die;
  defined(my $lookup_func_name = shift) or die;
  my @lines = grep { $_ ne '' } split("\n", $output);
  my $pretty_str = '';
  if ($lookup_func_name) {
    $pretty_str .= ' ' . $lines[0];
  }
  $pretty_str .= ' at ' . $lines[1];
  return $pretty_str;
}

my $binary = shift @ARGV;
if (! -x $binary || ! -r $binary) {
  die "Error: Cannot access executable ($binary)";
}

# Cache lookups to speed processing of files with repeated trace addresses.
my %addr2line_map = ();

# Disable stdout buffering
$| = 1;

# Reading from <ARGV> is magical in Perl.
while (defined(my $input = <ARGV>)) {
  if ($input =~ /^\s+\@\s+(0x[[:xdigit:]]{6,})(?:\s+(\S+))?/) {
    my $addr = $1;
    my $lookup_func_name = (!defined $2);
    if (!exists($addr2line_map{$addr})) {
      $addr2line_map{$addr} = `addr2line -ifC -e $binary $addr`;
    }
    chomp $input;
    $input .= parse_addr2line_output($addr2line_map{$addr}, $lookup_func_name) . "\n";
  }
  print $input;
}

exit 0;
