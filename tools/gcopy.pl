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
GetOptions( $cfg, @args=qw(start|s=s end|e=s storage|d=s
           match_undef|U! filterfile|F=s requestfile|R=s request|r=s filters|f=s dest_graph_file|g=s
           help|h select|S=s@ detach|D separate|P max_edges|me=s max_nodes|mn=s
           ) );

if ($cfg->{help}) {
  common::usage()
}
#$cfg->{storage}//='J:\projects\perl\kmg\scratch\db';
$cfg->{out}//='./gg_out.json';
$cfg->{defragment}//=1;
$cfg->{dest_graph_file}//='out';

my $base =  { name=>'detach_graph', testdata=>1,  request => {
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

my $req;
if ($cfg->{requestfile} || $cfg->{request}) {
  if ($cfg->{requestfile}=~/^-$/) {
    print STDERR "enter the request, end by dot:\n";
    while(<STDIN>) {
      last if (/^\.$/);
      $req.=$_;
    }
  }
  elsif ($cfg->{requestfile} ne '' ) {
    open my $fh, '<:utf8', $cfg->{requestfile} or  die$!;
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



my $g = query_graph_object->new($cfg);

my %stats;

my $result;

$req->{detach}=0;
$req->{separate}=0;
$req->{savedb}=0;

if ($cfg->{detach}) {
  $req->{detach}=1;
  $req->{savedb}=1;
  $result = $g->copy_subgraph( $cfg->{dest_graph_file}, $req )  ;
}
elsif ($cfg->{separate}) {
  $req->{separate}=1;
  $req->{savedb}=1;
  $result = $g->copy_subgraph( $cfg->{dest_graph_file}, $req )  ;
}
else {
  $result = $g->copy_subgraph( $cfg->{dest_graph_file}, $req )  ;
}
#$g->write_db();