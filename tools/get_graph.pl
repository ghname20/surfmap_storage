#!perl
#use Test::More tests => 7;
use strict;

use lib (q{E:\work\kmg\gather});
use lib (q{J:\projects\perl\kmg\gather});
use query_graph_object;
use Data::Dumper;
use Getopt::Long qw(:config no_ignore_case);
use Carp;
use JSON::XS;

$SIG{__DIE__} = sub { Carp::confess( @_ ) };
our $json=JSON::XS->new->canonical->relaxed;
our $cfg = {};
our @args;
GetOptions( $cfg, @args=qw(logpath=s capfilepath=s start|s=s end|e=s storage|d=s
           defragment|D match_undef|U! filterfile|F=s requestfile|R=s request|r=s filters|f=s testdata|t out|o=s
           max_edges|me=s max_nodes|mn=s shared|D=s help|h select|S=s@
           ) );

if ($cfg->{help}) {
  common::usage()
}
#$cfg->{storage}//='J:\projects\perl\kmg\scratch\db';
$cfg->{out}//='./gg_out.json';
$cfg->{defragment}//=1;

my $base =  { name=>'get_graph', testdata=>1,  request => {
    expand=>1 , match_undef=>$cfg->{match_undef}//0,
    max_edges=>$cfg->{max_edges},
    max_nodes=>$cfg->{max_nodes},
      filters=>
        $json->decode(<<'CUT'),
      [{"src_key":"^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E$"
      ,"dst_key":"^","start":"2016-01-10","end":"2016-02-06"}
      ,{"dst_key":"^\\Qurl:http://stackoverflow.com/tags/cytoscape.js/hot\\E$"
      ,"src_key":"^","start":"2016-01-10","end":"2016-02-06"}]
CUT
  }
};



my $g = query_graph_object->new($cfg);

my %stats;

my $req;
my $default_request_file = "defq.txt";
if ($cfg->{requestfile} || $cfg->{request} || (-f $default_request_file)) {
  if ($cfg->{requestfile}=~/^-$/) {
    print STDERR "enter the request, end by dot:\n";
    while(<STDIN>) {
      last if (/^\.$/);
      $req.=$_;
    }
  }
  elsif ($cfg->{requestfile} ne ''  || -f $default_request_file) {
    open my $fh, '<:utf8', $cfg->{requestfile}//$default_request_file or  die$!;
    read $fh, $req, -s $fh;
    close $fh;
  }
  elsif ($cfg->{request} ) {
    $req = $cfg->{request};
  }
  $req=~s/^\s*"|"\s*$//g;
  $req = $json->decode($req);
}
else {
  $req  = $base->{request};

  my $filters;
  if ($cfg->{filterfile}=~/^-$/) {
    print STDERR "enter the filter, end by dot:\n";
    while(<STDIN>) {
      last if (/^\.$/);
      $filters.=$_;
    }
  }
  elsif ($cfg->{filterfile} ne '' ) {
    open my $fh, '<:utf8', $cfg->{filterfile} or  die$!;
    read $fh, $filters, -s $fh;
    close $fh;
  }
  elsif ($cfg->{filters}) {
    $filters //= $cfg->{filters};
  }

  $filters=~s/^\s*"|"\s*$//g;
  $req->{filters} = $json ->decode($filters);
}

my $update_base = {
    request   => "update_node_info",
    direction => "in",
    operation => "merge",
  };

my $edges = [   {
    key   => "topic:test5 graph visualisation",
    node_type => "topic"
  },
  {
    peer_key   => "topic:test5 graph visualisation",
    peer_type  => 'topic',
    node_type => "query",
    key       => "query:test5 graph+visualization+javascript",
    link_type => 'session',
  },
  {
    peer_key   => "query:test5 graph+visualization+javascript",
        peer_type => "query",
    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/7034/graph-visualization-library-in-javascript",
    link_type => 'jump',

  }, {
    peer_key   => "query:test5 graph+visualization+javascript",
        peer_type => "query",

    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/24396708/zoomable-network-graph-in-angularjs",
    link_type => 'jump',
  },

  {
    key   => "topic:test5 cytoscape.js",
    node_type => "topic",
  },
  {
    peer_key=> "topic:test5 cytoscape.js",
    peer_type => "topic",
    key       => "query:test5 cytoscape.js+tutorial",
    node_type => "query",
    link_type => 'session',
  },
  {
    peer_key   => "query:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5http://stackoverflow.test/questions/21650711/live-code-examples-cytoscape-js-initialization-incomplete",
    link_type => 'jump',
  } ,
  {
    peer_key   => "query:test5 cytoscape.js+tutorial",
    peer_type => "query",
    node_type => "url",
    key       => "url:test5 http://chianti.ucsd.edu.test/~kono/cy3js/",
    link_type => 'jump',
  }
];

if ($cfg->{testdata}) {
  for my $e ( @$edges  ){
    my $req = { (%$update_base, %$e)}  ;
    $g->update_node_info($req);
  }
}



my $result = $g->get_graph_data( $req )  ;
my $test_result = {
    testdata=>$base->{testdata},
    name=>$base->{name},
    request=>$req,
    result=>$result,
  };

my $fh;

if ($cfg->{out} eq '-' ||$cfg->{out} eq ''  )  {
  $fh = \*STDOUT;
}
else {
  open($fh, '>:encoding(utf8)', "$cfg->{out}" ) or die $!;
}
print {$fh} $json->pretty->encode($test_result);
close $fh;
if ($g->changed()) {
  $g->write_db();
}
