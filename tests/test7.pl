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
    tests_get|tg=s no_init|I)
);

$cfg->{storage}      //= 'J:\projects\perl\kmg\tests\data\common';
$cfg->{resultpath}   //= 'J:\projects\perl\kmg\tests\data\test7\results';
$cfg->{expectedpath} //= 'J:\projects\perl\kmg\tests\data\test7\expected';
$cfg->{defragment}   //= 1;

if ( !$cfg->{no_init} ) {
  my $initial_core = 'J:\projects\perl\kmg\tests\data\common\query_graph_db.json';
  my $core         = $cfg->{storage} . "/core_graph.json";
  my $junk         = $cfg->{storage} . "/junk_graph.json";
  my $meta         = $cfg->{storage} . "/metadata_graph.json";
  unlink $core;
  unlink $junk;
  unlink $meta;
  File::Copy::copy( $initial_core, $core );
}

$cfg->{select} //= [qw(none core=Crw junk=rw)];

my $g = query_graph_object->new($cfg);

#$g ->get_new_requests();
#ok ( $g->consistency_check(), "data is consistent after get_new_requests");

#
#my $req_json = <<CUT;
#{"request_source":"popup",
#"request":"save_new_elements",
#"method":"save_new_elements"}
#CUT

#my $elem_json =

#my $req  = $json->decode($req_json);
#$req -> {elements} = $json ->decode($elem_json);
#$req -> {dryrun} = 1;

my %stats;

my $req_base = {
  request   => "update_node_info",
  direction => "in",
  operation => "merge",
};

my $edges = [
  {
    key       => "topic:test5 graph visualisation",
    node_type => "topic"
  },
  {
    peer_key  => "topic:test5 graph visualisation",
    peer_type => 'topic',
    node_type => "query",
    key       => "google:test5 graph+visualization+javascript",
    edge_type => 'session',
  },
  {
    peer_key  => "google:test5 graph+visualization+javascript",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/7034/graph-visualization-library-in-javascript",
    edge_type => 'jump',

  },
  {
    peer_key  => "google:test5 graph+visualization+javascript",
    peer_type => "query",

    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/24396708/zoomable-network-graph-in-angularjs",
    edge_type => 'jump',
  },

  {
    key       => "topic:test5 cytoscape.js",
    node_type => "topic",
  },
  {
    peer_key  => "topic:test5 cytoscape.js",
    peer_type => "topic",
    key       => "google:test5 cytoscape.js+tutorial",
    node_type => "query",
    edge_type => 'session',
  },
  {
    peer_key  => "google:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key =>
      "url:test5http://stackoverflow.test/questions/21650711/live-code-examples-cytoscape-js-initialization-incomplete",
    edge_type => 'jump',
  },
  {
    peer_key  => "google:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5 http://chianti.ucsd.edu.test/~kono/cy3js/",
    edge_type => 'jump',
  }
];

@{ $stats{first} }{qw(links nodes)} = ( $g->edge_count(), $g->node_count() );

unless ( $cfg->{notestdata} ) {
  print STDERR "updating with test data\n";
  for my $e (@$edges) {
    my $req = { ( %$req_base, %$e ) };
    my %tmpstats;
    my $lc  = $g->edge_count();
    my $nc  = $g->node_count();
    my $nlc = $req->{peer_key} ? $lc + 1 : $lc;
    my $nnc = $nc + 1;

    #@{$tmpstats{first}}{qw(links nodes)} = ($g->edge_count(), $g->node_count());

    $g->update_node_info($req);
    is( $nlc, $g->edge_count(), "link count correct: $req->{key}" );
    is( $nnc, $g->node_count(), "node count correct: $req->{key}" );

  }
}

my $tests_get = [
  {
    name     => 0,
    testdata => 1,
    request  => {
      match_undef => $cfg->{match_undef} // 0,

      filters => [
        {
          #'start' => '2016-01-20',
          #start => $cfg->{start}//'2016-01-10',
          'text' => 'cytoe|cyto0a',

          #'end' => $cfg->{end}//'2016-01-29',
        }
      ]
    },
  },

  {
    name     => 1,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      filters => [
        {

          "exact" => "true",
          "key"   => "url:http://stackoverflow.com/questions/26123468/dynamic-node-content-label-in-cytoscape-js",
          "start" => "2016-01-01",
          "end"   => "2016-02-01"
        }
      ],
      method         => "get_graph_data",
      request        => "get_graph_data",
      request_source => "popup",
    }
  },
  {
    name     => 2,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      match_undef => $cfg->{match_undef} // 0,
      expand => 1,

      filters => [
        {
          filter_type => "edge",
          "from"      => { "key" => "^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E\$" },
          to          => { "key" => "^" },
          "start"     => "2016-01-10",
          "end"       => "2016-02-06",
          edge        => { "edge_type" => "^" }
        },
        {
          filter_type => "edge",
          "to"      => { "key" => "^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E\$" },
          from          => { "key" => "^" },
          "start"     => "2016-01-10",
          "end"       => "2016-02-06",
          edge        => { "edge_type" => "^" }
        }
      ],
    },
  },
  {
    name     => 3,
    testdata => 1,
    "compute_relevance" => 0,
    expand   => 1,
    request  => {
      match_undef => $cfg->{match_undef} // 0,
      filters => [
        {
          filter_type => "edge",
          "from"      => {
            "key" => "^\\Qgoogle:cytoscape.js+add+node+right+click\\E\$"
          }
        }
      ],
    },
  },
  {
    name     => 4,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [ { filter_type => "edge", "from" => { "text" => "novosti" }, to => { "text" => "^" } } ],
    },
  },
  {
    name     => 5,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [ { filter_type => "edge", "from" => { "text" => "novosti" }, to => { "text" => "^" } } ],
    },
  },
  {
    name     => 6,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [
        {
          filter_type => "edge",
          "from"      => { "key" => "\\\\Qgoogle:test5 graph+visualization+javascript\\\\E" },
          to          => { "text" => "^" }
        }
      ],
      method         => "get_graph_data",
      request        => "get_graph_data",
      request_source => "popup",
    }
  },
  {
    name     => 7,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [
        {
          filter_type => "edge",
          "to"      => {
            "key" =>
              "^\\Qurl:test5http://stackoverflow.test/questions/24396708/zoomable-network-graph-in-angularjs\\E\$"
          },
          from => { "text" => "^" }
        }
      ],
    }
  },
  {
    name     => 8,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters     => [
        {
          filter_type => "edge",
          "from"      => { "key" => "^\\Qtopic:test5 graph visualisation\\E\$" },
          to          => { "text" => "^" }
        }
      ],
    }
  },
  {
    name     => 9,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      expand      => 1,
      match_undef => $cfg->{match_undef} // 0,
      filters =>

        [
        {
          filter_type => "edge",
          "from"      => { "key" => "^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E\$" },
          ,
          to      => { "key" => "^" },
          "start" => "2016-01-10",
          "end"   => "2016-02-06"
        }

        ,
        {
          filter_type => "edge",
          "to"      => { "key" => "^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E\$" },
          ,
          from      => { "key" => "^" },
          "start" => "2016-01-10",
          "end"   => "2016-02-06"
        }
        ],

    }
  },
  {
    name     => 10,
    testdata => 1,
    "compute_relevance" => 0,
    request  => {
      "expand"  => 1,
      "filters" => [

        {
          to            => { "text" => "^" },
          "filter_type" => "edge",
          from          => {
            "source"   => "browser",
            "text" => "^"
          }
        },
        {
          to => {
            "text" => "^",
          },
          "filter_type" => "edge",
          from          => {
            "source"   => "browser",
            "text" => "^"
          },
        }
      ],
      "request"        => "get_graph_data_with_stats",
      "request_source" => "popup"
      }

  },

];

my $tests_get_subset = [ 0 .. $#$tests_get ];
if ( $cfg->{tests_get} ne '' ) {
  $tests_get_subset = [ split ',', $cfg->{tests_get} ];
}

print STDERR "testing filters\n";

for ( my $i = 0 ; $i < @$tests_get_subset ; $i++ ) {
  my $ti   = $tests_get_subset->[$i];
  my $test = $tests_get->[ $tests_get_subset->[$i] ];
  my $req  = $test->{request};
  my $result;
  my ( $lc, $nc );
  $result = $g->get_graph_data($req);
  $lc     = $g->edge_count();
  $nc     = $g->node_count();
  my $test_result = {
    testdata   => $test->{testdata},
    name       => $test->{name},
    test       => $ti,
    request    => $req,
    result     => $result,
    node_count => $nc,
    edge_count => $lc,
  };
  my $fh;

  if (!$cfg->{'no-resout'} ) {
    open( $fh, '>:raw', "$cfg->{resultpath}/test_$tests_get_subset->[$ti].json" ) or die $!;
  }
  else {
    $fh = \*STDOUT;
  }
  print {$fh} $json->pretty->encode($test_result);
  close $fh;
  if ( !$cfg->{'no-compare'} ) {
    open( my $fh, '<:raw', "$cfg->{resultpath}/test_$tests_get_subset->[$ti].json" )   or die $!;
    open( my $gh, '<:raw', "$cfg->{expectedpath}/test_$tests_get_subset->[$ti].json" ) or die $!;
    read( $fh, my $resultjson,   -s $fh ) or die $!;
    read( $gh, my $expectedjson, -s $gh ) or die $!;
    close $fh;
    close $gh;
    my $result   = $json->decode($resultjson);
    my $expected = $json->decode($expectedjson);
    is_deeply( $result, $expected, "request $i: the result matches expected data" );
  }
}

done_testing();
