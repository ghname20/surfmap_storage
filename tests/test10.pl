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

$SIG{__DIE__} = sub { Carp::confess(@_) };
our $json = JSON::XS->new->canonical;
our $cfg  = {};
GetOptions(
  $cfg,
  qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s defragment|D match_undef|U! filter|f=s notestdata|T no-resout|O no-compare|C
    no_init|I)
);

$cfg->{logpath}      //= 'J:\projects\perl\kmg\tests\data\test10\logs';
$cfg->{storage}      //= 'J:\projects\perl\kmg\tests\data\test10\db';
$cfg->{resultpath}   //= 'J:\projects\perl\kmg\tests\data\test10\results';
$cfg->{expectedpath} //= 'J:\projects\perl\kmg\tests\data\test10\expected';

if ( !$cfg->{no_init} ) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\test10\db\core_graph.json.0';
  my $initial_junk = 'J:\projects\perl\kmg\tests\data\test10\db\referer160101.json.0';
  my $core         = $cfg->{storage} . "/core_graph.json";
  my $junk         = $cfg->{storage} . "/referer160101_graph.json";
  my $meta         = $cfg->{storage} . "/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy( $initial_core, $core );
  File::Copy::copy( $initial_junk, $junk );
}

$cfg->{select} //= [qw(none core=Crw referer160101=rw)];

my $request = {
  "expand"  => 1,
  "filters" => [
    {
      "end"         => "2016-03-08",
      "filter_type" => "edge",
      "from"        => {
        "text" => "windows.*update"
      },
      "start" => "2016-01-10"
    },
    {
      "end"         => "2016-03-08",
      "filter_type" => "edge",
      "start"       => "2016-01-10",
      "to"          => {
        "text" => "ultrasound"
      },
      edge=>{
        edge_type=>"^(?!special)"
      }
    }
  ],
  detach => 1
};



my $fh;
my $g = query_graph_object->new($cfg);
handle_results( 'stats0_after_init', my $stats = $g->get_stats() );
handle_results( 'metadata0_after_init', $g->get_metadata() );

my $key1;
my $nid1 = $g->g->get_node_by_key("google:abdomen+ultrasound+scan++FOV+parameter+\"1..10000+comments\"");
my $nid2 = $g->g->get_node_by_key("google:abdominal+ultrasound+scan+parameters");
my $nidc= $g->g->get_node_by_key("url:http://www.wikiradiography.net/page/Abdomen+Normal+Measurements");

my $save_req = {
  testdata    => 1,
  node_change => 1,
  link_change => 1,
  request     => {
    request_source => "popup",
    request        => "save_new_elements",
    method         => "save_new_elements",
    elements       => [
      { "id" => "n".$nidc->id},
      { "id" => "newnode_86", "name" => "newnode_86", "node_type" => "topic", "degree" => 20, "key" => "topic:ultrasound", "comment"=>"topic:initial", topic=>"ultrasound" },
      { "id" => "newnode_87", "name" => "newnode_87", "node_type" => "topic", "degree" => 20, "key" => "topic:unrelated", "comment"=>"topic:unrelated" , topic=>"unrelated"},
      { "id" => "newedge_63", "source" => "newnode_86", "target" => "n".$nidc->id, "edge_type" => "related" },
      { "id" => "newedge_65", "source" => "newnode_87", "target" => "n".$nidc->id, "edge_type" => "related" },
    ],
  },
};


#"query" : "abdomen+ultrasound+scan++FOV+parameter+\"1..10000+comments\"",
#abdominal+ultrasound+scan+parameters

my $result = $g->save_new_elements($save_req->{request});
$g->copy_subgraph( 'out', $request );
sleep(1);
$g->write_db();
handle_results( 'stats1_after_save_and_copy--detach', $stats = $g->get_stats() );
handle_results( 'metadata1_after_save_and_copy--detach', $g->get_metadata() );

$cfg->{select} = [qw (none core=Crw)];
$g = query_graph_object->new($cfg);
handle_results( 'stats2_for_core_alone', $stats = $g->get_stats() );
handle_results( 'metadata2_for_core_alone', $g->get_metadata() );

my $nid3 = $g->g->get_node_by_key("url:http://www.wikiradiography.net/page/Abdomen+Normal+Measurements");

my $save_req2 = {
  testdata    => 1,
  node_change => 1,
  link_change => 1,
  request     => {
    request_source => "popup",
    request        => "save_new_elements",
    method         => "save_new_elements",
    elements       => [
      { "id" => "n".$nid3->id, "name" => "n".$nid3->id,  "degree" => 20, "comment"=>"test node updated" },
    ],
  },
};
$result = $g->save_new_elements($save_req2->{request});
$g->write_db();
handle_results( 'stats3_after_save_to_core', $stats = $g->get_stats() );
handle_results( 'metadata3_after_save_to_core', $g->get_metadata() );

$cfg->{select} = [qw (none out=Crw)];
$g = query_graph_object->new($cfg);
handle_results( 'stats4_for_out_alone', $stats = $g->get_stats() );
handle_results( 'metadata4_for_out_alone', $g->get_metadata() );

my $nid4 = $g->g->get_node_by_key("url:http://www.wikiradiography.net/page/Abdomen+Normal+Measurements");
my $topic=$nid4->data->{comment};
is($topic, "test node updated", "topic must be read from core\n");
done_testing();

sub remove_volatile_data {
  my ($data) = @_;
  return if ref $data ne 'HASH';
  return if !exists $data->{storage} || ref $data->{storage} ne 'HASH';
  my $storage = $data->{storage};
  for my $s ( keys %$storage ) {
    if ( exists $storage->{$s}{timestamp} ) {
      delete $storage->{$s}{timestamp};
    }
  }
}

sub handle_results {
  my $fh;
  my ( $what, $stats ) = @_;
  if ( !$cfg->{'no-resout'} ) {
    open( $fh, '>:raw', "$cfg->{resultpath}/$what.json" ) or die $!;
    print {$fh} $json->pretty->encode($stats);
    close $fh;
  }
  if ( !$cfg->{'no-compare'} ) {
    my $t1 = "$cfg->{resultpath}/$what.json";
    open( my $fh, '<:raw', "$t1" )   or die "$t1:$!";
    my $t2 = "$cfg->{expectedpath}/$what.json";
    open( my $gh, '<:raw', $t2 ) or die "$t2: $!";
    read( $fh, my $resultjson,   -s $fh ) or die $!;
    read( $gh, my $expectedjson, -s $gh ) or die $!;
    close $fh;
    close $gh;
    my $result = $json->decode($resultjson);
    remove_volatile_data($result);
    my $expected = $json->decode($expectedjson);
    remove_volatile_data($expected);
    is_deeply( $result, $expected, "$what: the result matches expected data" );
  }
}
