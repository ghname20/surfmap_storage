#!perl
##############
############## testing query filters
#use Test::More tests => 7;
use strict;
use Test::More;
use Test::Deep;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Carp;
use JSON::XS;

$SIG{__DIE__} = sub { Carp::confess( @_ ) };
our $json=JSON::XS->new->canonical;
our $cfg = {};
GetOptions( $cfg, qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s defragment|D match_undef|U! filter|f=s notestdata|T resout|o no-compare|C
           no_init|I) );

$cfg->{storage}//='J:\projects\perl\kmg\tests\data\test9\db';
$cfg->{resultpath}//='J:\projects\perl\kmg\tests\data\test9\results';
$cfg->{expectedpath}//='J:\projects\perl\kmg\tests\data\test9\expected';

if (!$cfg->{no_init}) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\test9\db\core_graph.json.0';
  my $initial_junk= 'J:\projects\perl\kmg\tests\data\test9\db\referer160101.json.0';
  my $core =$cfg->{storage}."/core_graph.json";
  my $junk =$cfg->{storage}."/referer160101_graph.json";
  my $meta=$cfg->{storage}."/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy($initial_core, $core);
  File::Copy::copy($initial_junk, $junk);
}

$cfg->{select}//=[qw(none core=Crw referer160101=rw)];


my $request= {
   "expand" => 1,
   "filters" => [
      {
         "end" => "2016-03-08",
         "filter_type" => "edge",
         "from" => {
            "text" => "windows.*update"
         },
         "start" => "2016-01-10"
      },
      {
         "end" => "2016-03-08",
         "filter_type" => "edge",
         "start" => "2016-01-10",
         "to" => {
            "text" => "ultrasound"
         }
      }
   ],
   detach=>1
};

sub remove_volatile_data {
  my ($data)=@_;
  return if ref $data ne 'HASH';
  return if !exists $data->{storage} || ref $data->{storage}ne 'HASH';
  my $storage = $data->{storage};
  for my $s (keys %$storage) {
    if (exists $storage->{$s}{timestamp}) {
      delete $storage->{$s}{timestamp}
    }
  }
}

sub handle_results {
  my $fh;
  my ($what, $stats)=@_;
  if ($cfg->{resout}) {
    open($fh, '>:raw', "$cfg->{resultpath}/$what.json" ) or die $!;
    print {$fh} $json->pretty->encode($stats);
    close $fh;
  }
  if (!$cfg->{'no-compare'}) {
    open(my $fh, '<:raw', "$cfg->{resultpath}/$what.json" ) or die $!;
    open(my $gh, '<:raw', "$cfg->{expectedpath}/$what.json" ) or die $!;
    read ($fh, my $resultjson, -s $fh) or die $!;
    read ($gh, my $expectedjson, -s $fh) or die $!;
    close $fh; close $gh;
    my $result = $json ->decode($resultjson);
    remove_volatile_data($result);
    my $expected = $json ->decode($expectedjson);
    remove_volatile_data($expected);
    is_deeply($result, $expected, "$what: the result matches expected data");
  }
}

my $fh;
my $g = query_graph_object->new($cfg);
handle_results('stats_after_init', my $stats=$g->get_stats());
$g->copy_subgraph('out', $request);
handle_results('stats_after_copy--detach', $stats=$g->get_stats());
$g->write_db();
$cfg->{select}=[qw (none core=Crw referer160101=rw out=rw) ];
$g = query_graph_object->new($cfg);
handle_results('stats_after_init_merge', $stats=$g->get_stats());
my $md=  $g->get_metadata();
handle_results('final_metadata', $md);
done_testing();
