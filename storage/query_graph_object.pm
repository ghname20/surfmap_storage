#!perl
package query_graph_object;
use strict;
use utf8;
#use warnings; # FIX:
#use Moose;
#use Moose::Meta;
use strict;
use common;
use XML::Twig;
use Encode;
use File::Basename;
use JSON::XS;
use Data::Dumper;
use File::Copy;
use Exporter qw(import);
use PDL qw(pdl statsover);
our @EXPORT=qw(&get_new_requests &get_sorted &read_db &write_db &init_db);
use Carp;
use List::MoreUtils;
use lib q'J:\projects\perl\kmg\debug';
use Clone;
use graph_object;
use kmg_stats;
use Data::Compare;
use Lingua::Stem::Snowball ;

our $json=JSON::XS->new->canonical->convert_blessed;
PDL::no_clone_skip_warning();
my $db_encoding ="utf8";
my $cap_encoding ="utf8";
my $log_encoding ="raw";

my $stem_ru= Lingua::Stem::Snowball->new( lang => 'ru', encoding=>'UTF-8' );
my $stem_en= Lingua::Stem::Snowball->new( lang => 'en', encoding=>'UTF-8' );

sub enc {
  my ($enc)=@_;
  $enc eq 'raw' ? ":$enc": ":encoding($enc)";
}

my $def_guid = 'FA50B682-6FA5-1014-95DF-59D5B0504476';
my $rdef= qr{(?(DEFINE)(?'divs'(?:
          (?>(?:(?!(?:</?div)).)*)
          |
          <div(?&divs)</div>
        )*))}msix;
my $spandef= qr{(?(DEFINE)(?'spans'(?:
          (?>(?:(?!(?:</?span)).)*)
          |
          <span(?&spans)</span>
        )*))}msix;


my $order = [qw(topic query url)];

=pod
my @link_types = (
     [ qw ( browser tab tab tab2tab) ]
    , [ qw ( browser query tab query2tab) ]
    , [ qw ( browser tab query tab2query) ]
    , [ qw ( browser query query query2query) ]
    , [ qw ( fiddler query result query2result) ]
  );
=cut

# map: node field; filter field

my @rxkey_map = (
  [qw(node_type node_type)],
  [qw(comment text)],
  [qw(comment comment)],
  [qw(source source)],
  [qw(rating text)],
  [qw(rating rating)],
  [qw(key text)],
  [qw(url text)],
  [qw(final_url text)],
  [qw(key key)],
  [qw(link_type link_type)],
  [qw(edge_type edge_type)],
);

my $rxkey_to_field={};
for (my $i=0;$i<@rxkey_map; $i++) {
  push @{$rxkey_to_field->{$rxkey_map[$i][1]}}, $rxkey_map[$i][0];
}

#my $default_log_dir = q{F:\work\fiddler\logs};
#my $default_req_file_dir = q{F:\work\fiddler\capture};
my $default_cfg={
  use_twig=>0,
  output=>'cp866',
  storage => 'J:\projects\perl\kmg\db',
  verbose=>1,
  defragment=>0,
  capfilepath=>'F:\work\fiddler\capture',
  logpath=>'F:\work\fiddler\logs',
  max_nodes=>300,
  max_edges=>300,
  core=>'rw',
  junk=>'rw',
  debug=>0,
  ignore_dups=>1,
  rescan_results=>0,
  request_ts_range=>10,
  merge_method=>1,
};

my $default_metadata;


1;

sub next_id { shift->{next_id}++} ;
sub set_id {
  my ($self,$id)=@_;
  $self->{next_id}=$id+0;
};
sub get_id { shift->{next_id}};


sub new {
	my ($class, $cfg)=@_;
	$cfg={ %$default_cfg, %{$cfg//{}} };
  $cfg->{start}//='2016-01-08';
  my $start= $cfg->{start} && common::parsetime($cfg->{start}) ;
  my $end= $cfg->{end} && common::parsetime($cfg->{end});
  my $g = graph_object->new(common::sub_hash( $default_cfg, graph_object::default_cfg() )  ); # TODO: check if graph_object needs cfg
  my $self=bless { start=>$start, end=>$end, cfg=>$cfg, requests=>0, g=>$g, next_id=>0} ;
	$self->init_db();
  return $self;
}

sub g  {
  my ($self,$g)=@_;
  return $self->{g}=$g if (defined $g );
  return $self->{g};
}

#changed
sub changed  {
  my ($self,$changed,$partition)=@_;
  if (defined $changed) {
    #$self->{changed}=$changed;
    if (defined $partition) {
      return $self->{_selection}{$partition}{changed}=$changed;
    }
    return $self->{changed}=$changed;
  }
  if (defined $partition) {
    return $self->{_selection}{$partition}{changed};
  }
  return $self->{changed};
}

sub request_files  {
  my ($self,$request_files)=@_;
  return $self->{request_files}=$request_files if (defined $request_files );
  return $self->{request_files};
}


sub consistency_check {
  my ($self,$args)=@_;
  my $rc = $self->g->consistency_check();

  return $rc;
  #$args//={check_nodes=>1, check_links=>1};
  #my $nodes= $self->{nodes};
  #my $links= $self->{links};
  my (%seen_from, %seen_to, %children_actual, %children_expected);
  #for (my $i=0;$i<$self->$nodes;$i++) {
  my $ok;
  my $descendants={};
  my $proc = sub {
    my ($id, $node, $parent, $root, $level)=@_;
    my $data;

    my $anc ;
    if ($node==$root) {
      $anc = sprintf 'no parent, root id=%s', $root->id;
    }
    elsif ($parent==$root) {
      $anc = sprintf 'parent is root: root id=%s', $root->id;
    }
    else {
      $anc = sprintf 'parent id=%s, root id=%s', $parent->{id}, $root->id;
    }

    if ($node==$root) {
      if (ref $node ne 'graph_object::node') {
        die sprintf "wrong object: '%s' !='graph_object::node': ancestry: %s", ref $node, $anc;
      }
=pod
      if (scalar(keys %$descendants) !=  scalar(keys %{$root->data->{descendants}})) {
        die sprintf "descendant count mismatch: root:%s <> actual:%s"
        , scalar(keys %$descendants) , scalar(keys %{$root->data->{descendants}});
      }
=cut
      for my $did(keys %{$root->{descendants}}) {
        my $dnode = $root->{descendants}{$did};
        if (!$descendants->{$did}) {
          die sprintf "descendant %s is missing from hierarchy: ancestry %s", $did, $anc
        }
        #if ($root->data->{descendants}{$did}) {
        #  die sprintf "wrong object: '%s' !='graph_object::node': ancestry: %s", ref $node, $anc ;
        #}
      }

    }
    else {
      if (ref $node ne 'HASH') {
        die sprintf "child node %s: wrong ref '%s' !='HASH': ancestry: %s", $id, ref $node, $anc ;
      }
      #my $data = $node->{data};
      #if (ref $data ne 'HASH') {
      #  die sprintf "child node %s: wrong data ref: '%s' !='HASH': ancestry %s", $id, ref $data, $anc;
      #}

      if ($node->{id} ne $id) {
        die sprintf "child node %s: wrong id %s: ancestry %s", $id, $node->{id}, $anc;

      }
      if ($node->{parent} != $parent->{id}) {
        die sprintf "child node %s: wrong parent %s(%s): actual parent %s(%s), parent:%s", $id
          , $node->{parent}{id}, $node->{parent}, $parent->{id}, $parent
      }

      if ($root->{descendants}{$node->{id}} != $node) {
        die sprintf "node %s descendancy data is wrong: %s!=%s, parent: %s", $id
        , $root->{descendants}{$node->{id}} , $node, $root->id, $anc;
      }
      $descendants->{$node->{id}}++;
    }

    0
  };

  for my $nid (sort keys %{$self->g->nodes}) {
    #my $parent = $data->{parent};
    my $node;
    if (!defined ($node = $self->g->get_node_by_id($nid))) {
      die sprintf "node with id $nid is missing";
    }

    $ok=0;

    graph_object::node::descend_dl($node, undef, $node, $proc,0);
=pod
    if (my $p = $self->get_node($data->{parent})) {
      push @{$children_actual{$p}}, $i;
    }




    next unless my $children= $data->{children};
    $children_expected{$i}=@$children;
    for my $ch_key (@$children) {
      my ($chdata,$chref,$ch)= $self->get_data($ch_key);
      if (!$chdata) {
        die sprintf "child does not exist: data[$i]->$ch_key-----\n%s", Dumper($noderef);
      }
      if (get_node($self,$chdata->{parent}) ne $i) {
        die sprintf "child has wrong parent: {$ch_key}[$ch]\->$chdata->{parent}!=$i-----\n%s\n------\n%s", Dumper($noderef), Dumper($chref);
      }
      #if ($data->{parent}) {
      #  push @{$children_actual{$i}}, $ch;
      #}
    }
=cut
  }

=pod
  for my $parent (sort keys %children_actual) {
    #my $ch_actual= $children_actual{$parent} && @{$children_actual{$parent}};

    if ($children_expected{$parent}!=@{$children_actual{$parent}}) {
      my $pref = $nodes->[$parent] && $nodes->[$parent][0];
      my @chs= $pref && $pref->{children} ?@{$pref->{children}}  : ();
      die sprintf "children count mismatch: children of %s: %d[%s]<=>%d[%s]\n-----\n%s"
      , $parent, $children_expected{$parent}
      , join (',', $children_expected{$parent} ?  @chs : ()), 0+@{$children_actual{$parent}}
              , join (',', @{$children_actual{$parent}})
              ,  Dumper($nodes->[$parent][0]);
    }
  }
=cut
  return 1;
}

sub results {
  my ($self,$results)=@_;
  return $self->{results}=$results if (defined $results );
  return $self->{results};
}

=pod
#args: (fixed partition, volatile partition)
sub current {
  my ($self,@current)=@_;
  if (@current>0) {
    $self->{current}=$current[0];
  }
  if (@current>1) {
    $self->{_current}=$current[1];
  }
  return $self->{current}//$self->{_current};
}
=cut

#current
sub current {
  my ($self,$current)=@_;
  return $self->{current}=$current if (defined $current );
  return $self->{current};
}

sub get_file_info {
	my ($self,$filepath)=@_;

	open(my $fh, "<".enc($log_encoding), $filepath) or return ();
	my $str=<$fh>;
	close $fh;
	$str=~s{[\r\n]+$}{};
	return () if $str =~/^\s*$/;
	my $file = $self->{request_files}->{$str} || { modtime=>undef, files=>{}, nextline=>0};
	return ($file, $str);
}

sub save_file_info {
	my ($self,$file_info,$filepath,$file_id)=@_;
	if (!$file_info->{files}{$filepath}) {
		$file_info->{files}{$filepath} = 0 + keys %{$file_info->{files}};
	}
	$self->{request_files}->{$file_id}=$file_info;
}

sub filter_ts {
  my ($self,$nodes,$start,$end,$stop)=@_;
  return () if !ref $nodes ne 'ARRAY';
  my $in_range;
  my $filter= sub {
    my ($data)=@_;
    return 0 if (ref $data ne 'HASH');
    if (!defined $start or $data->{ts} >=$start) {
      if (!defined $end or $data->{ts} >=$end) {
        ++$in_range;
        return $stop;
      }
    }
    0;
  };
  my @match;
  for my $node (@$nodes) {
    next if (ref $node != 'graph_object::node');
    $in_range=0;
    $node->descend_df($filter);
    push @match, $node if $in_range;
  }
}

sub node_ts {
  my ($self,$node,$stop)=@_;
  return () if ref $node ne 'graph_object::node';
  my $in_range;
  my ($start, $end);
  my $filter= sub {
    my ($id,$child,$parent,$root,$level)=@_;
    #my ($data)=@_;
    return 0 if (ref $child ne 'HASH');
    my $ts = $child->{data}{ts};
    if ($ts) {
      $start=  $ts if (!defined $start || $start > $ts) ;
      $end =  $ts if (!defined $end || $end < $ts) ;
      if ($stop) {
        return 1;
      }
    }
    0;
  };
  $node->descend_df(undef, $node, $filter, 0);
  #$node->descend_df($filter);
  return wantarray ? ($start,$end) : $start;
}


sub get_new_requests {
  my ($self)=@_;
  #printf STDERR "get_new_requests: %s: %s\n", ref $self, Dumper({start=>$self->{start}, end=>$self->{end}, cfg=>$self->{cfg} } );
  opendir my $D, $self->{cfg}{logpath} or die$!;
	my $totallines=0;
	my $changes=0;
  my $warned_referer;
  my $fiddler_include= $self->{cfg}{fiddler_include}//'query';


  while (my $f = readdir $D) {

    my $filepath = "$self->{cfg}{logpath}/$f";
    next if -d $filepath;
    next if ! -f $filepath;
    next unless $f =~ m{^(\d+|current)\.txt$}i;
    my ($file_info, $file_id) = $self->get_file_info($filepath);
		next if !$file_info;
    my @stat = stat($filepath);
    do {warn "no stat : $filepath" ; next }
      if (!defined $stat[9]);

		if (!$self->{cfg}{rescan_results} && $file_info->{modtime} && $file_info->{modtime} == $stat[9]) {
      next; # not processing files that haven't changed
    }

    next if $self->{start} && $self->{start} > $stat[9];

    open(my $fh, "<".enc($log_encoding), $filepath) or die $!;
		my $nextline = $file_info->{nextline} || 0;
		my $line=0;
    while (<$fh>) {
			next if $line++ < $nextline && !$self->{cfg}{rescan_results};
			$totallines++;

      s{[\r\n]+$}{};
#2015-12-27 11:48:24,364 #13542	http://plus.google.com:443	200	F:\work\fiddler\capture\13542_Status200.txt.79df4708-d0d6-4b36-8b85-d002bb8b49df
#2015-12-27 14:55:07,595 #15088	https://www.google.com/search?client=opera&q=soap%3A%3Alite+dump+requests&sourceid=opera&ie=UTF-8&oe=UTF-8	200	F:\work\fiddler\capture\15088_.htm.b7c7d07f-7fe8-4dec-bb3e-431c3e9b236f
			my ($ts, $no, $url, $status, $result_file, $guid, $referer);
      unless (($ts, $no, $url, $status, $result_file, $guid, $referer) = m{^(?'ts'\d{4}-\d\d\-\d\d\s+\d\d:\d\d:\d\d)(?:,\d+)?
				\s+\#?(?'no'\d+)\t(?'url'[^\t]*)\t(?'status'[^\t]*)
				\t(?:(?'file'[^\t]*?\.([\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}))
					 (?:\t(?'ref'[^\t]*?))?)?\s*$}x) {
						unless (($ts, $no, $url, $status, $referer) = m{^(?'ts'\d{4}-\d\d\-\d\d\s+\d\d:\d\d:\d\d)(?:,\d+)?
				\s+\#?(?'no'\d+)\t(?'url'[^\t]*)\t(?'status'[^\t]*)
				(?:\t(?'ref'[^\t]*?))?\s*$}x) {
							next;
						}
					}

      my $req_time = common::parsetime($ts);

      next if $self->{start} && $self->{start} > $req_time;
      next if $self->{end} && $self->{end} <= $req_time;
=pod
      if ($seen{"$ts#$no"}) {
        next;
      }
      $seen{"$ts#$no"}++;
=cut

      # 2016-01-03 06:40:08,373 #11963	https://www.google.com/search?client=opera&q=dreamweaver+using+generated+code&sourceid=opera&ie=UTF-8&oe=UTF-8
#  200	F:\work\fiddler\capture\11963_.htm.6f632de5-7531-4607-a280-f36b712fea21

# 2016-01-03 08:52:24,313 #89	https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=4&ved=0ahUKEwiV4qLIm4zKAhXlmnIKHa22A9AQFggwMAM&url=http%3A%2F%2Fstackoverflow.com%2Fquestions%2F13159846%2Fdreamweaver-cs5-5-and-php-generated-code&usg=AFQjCNEm76GwsHMNjhxKzPjEan9aw-02iA&sig2=fgT3MKULnBMPNjAv93CvLg	200	https://www.google.com/

# 2016-01-03 08:52:26,813 #91	http://stackoverflow.com/questions/13159846/dreamweaver-cs5-5-and-php-generated-code	200	https://www.google.com/


			$changes++;
			my ($key_to, $from , $g_url, $node_type, $edge_type) ;
      #my ($from, $peer, $edge_type );

      #my $node=add_node($self,{source=>'fiddler', node_type=>'request', ts=>common::parsetime($ts)});
			#my $urlkey = "url:$url";
			#my $url_node= get_node($self,$urlkey) || get_or_add_node($self,$urlkey, {node_type=>'url', url=>$url});

      my $allowed=0;

      my ($query, $host, $topic);
      if ($url=~ m{^\w+://www.google.(com|ru)/url\?.*?(?<=&|\?)url=([^&]*).*$}) {
        $node_type='url';
        $g_url=$url;
				$url = url_unescape($2);
        $key_to = "url:$url";
      }
      elsif (($host, $query) = $url =~ m{^\w+://(www\.google\.(?:com|ru))/search.*?[?&]q=([^&]*)} ) {
        #$query=~s{%([\da-f]{2})}{chr hex $1}egi;
        #$query=decode('utf8',$query);
        $query=url_unescape($query);
        $node_type = 'query';
        $key_to="google:$query";
        $allowed=1;
      }
      else {
        $node_type='url';
        $key_to = "url:$url";
      }


      ## checking referer

      if ($referer ne '') {
        if ($referer =~ m{^\w+://www\.google\.(com|ru)\b}) {
          my $found=find_result($self,$url,undef,common::parsetime($ts));
          my $filtered = $self->filter_ts($found,0);

          $from = ($filtered && $filtered->[0]);
          if ($from) {
            $edge_type='jump1';
            $allowed=1;
          }

  #my $edge=graph_object::edge->new2($data, $from->id, $to->id, undef, $er->[1], $er->[2]);

        }
        elsif ($self->{cfg}{fiddler_include}=~m{\b(referer)\b}) {
          if (!$warned_referer++) {
            warn "incliding referer navigation\n"
          }
          my $rkey = "url:$referer";
          $from = $self->g->get_node_by_key($rkey);
          if ($from ) {
            my $data = $from->data;
            #unless ($data->{node_type} eq 'url' && $data->{source} eq 'fiddler') {
            $allowed=1;
            $edge_type='referer';
          }
        }
				#add_peers($self, $peer_type,[$peer] , [$node]) if $peer && $node;
			}

      if ($fiddler_include=~/^\bany\b/) {
        $allowed=1;
      }

      next unless $allowed;

      my $to= $self->g->get_node_by_key($key_to);
      if (!defined $to ){
        my $meta= { partition=>$self->current, id=>$self->next_id
																			 , data=>{node_type=>$node_type, source=>'fiddler'}};
        $meta->{data}{query}=$query if defined $query;
        $meta->{data}{url}=$url if defined $url;
        $to = graph_object::node->new($meta);

      #unless ($fiddler_include=~/\b($node_type)\b/) {
      #  next;
      #}
      #
        if (!defined $self->g->add_node( $to )) {
          die "failed to add the node\n";
        }
      }

      my $its= common::parsetime($ts);
      my $children = $to->find_children({ start_ts=>$its-$self->{cfg}{request_ts_range}, end_ts=>$its+$self->{cfg}{request_ts_range}});
      my $child_id;
      if (!$children) {
        my $data=  {source=>'fiddler', ts=>$its};
        $child_id = $to->add_child($data,$to->id, $self->next_id);  # FIX: correct id after read_db
        $self->changed(1);
        if (!defined $child_id) {
          die sprintf "!defined \$child_id for %s, %s ", $key_to, $to->id;
        }
      }
      else {
        $child_id = shift @$children;
      }

      if ($edge_type ne '' && $from) {
            my $meta = {  data=>{edge_type=>$edge_type}, from=>$from->id, to=>$to->id, id=>$self->next_id,
               to_child=>$child_id, partition=>$self->current };

            my $edge=graph_object::edge->new($meta);
            $self->g->add_edge( $edge);

      #      my $meta = {  data=>{edge_type=>$edge_type}, from=>$from->id, to=>$to->id
      #              , id=>$self->next_id
      #              , to_child=>$child_id, partition=>$self->current };
      #        my $edge=graph_object::edge->new($meta);
      #        $self->g->add_edge( $edge);
      #}

      }

      if ($node_type eq 'query' && $guid ne '' && $result_file ne '' ) {
        $self->add_results($to, $child_id, $guid, $result_file);
      }
		}

		close $fh;
		if ($line <= $file_info->{nextline}) {
			$line =$file_info->{nextline};
		}

		$file_info->{modtime}=$stat[9];
		$file_info->{nextline}=$line;
		printf STDERR "%s: %s lines\n", $filepath, $line-$nextline;
		save_file_info($self, $file_info, $filepath, $file_id);
	}
	#print STDERR "total lines: $totallines\n" if $totallines > 0;
  if ($changes) {
    $self->changed(1);
  }
	return $changes;
}

sub find_result {
	my($self,$res_url, $start, $inc_end)=@_;
	my $rr=$self->{results}->{$res_url};
  #$self->results->{$url}{$node->id}{$request_id}=$i;
  my $found=[];
  while(my ($node_id,$req_hash)= each %{$rr}) {
    my $node= $self->g->get_node_by_id($node_id);
    if (!$node) {
      die "node $node_id missing";
    }
    for my $req_id (keys %$req_hash) {
      my $ch = $node->get_child($req_id);
      if (!$ch) {
        die "node $node_id, child $req_id missing";
      }
      my $ts;
      if (!defined ($ts = $ch->{data}{ts})) {
        die "node $node, child $req_id: not defined {ts}"
      }
      if ((!defined $start || $ts >=$start)
            && (!defined $inc_end || $ts <= $inc_end)) {
        #my $result = $node->data->{results}{$req_id}[$req_hash->{$req_id}];
        my $result = $ch->{data}{results}[$req_hash->{$req_id}];
        if (ref $result ne 'HASH') {
          die "node $node_id, child $req_id, result $req_hash->{$req_id}: not a HASH";
        }
        push @$found, [$node_id, $req_id, $req_hash->{$req_id}, $result];
      }
    }
  }
  return () if !@$found;
  return $found;

}


sub url_unescape{
  my ($url)=@_;
  #$url=~s{%(..)}{chr hex $1}ge;
  #$url=decode('utf8',$url);
  $url=~s{%([\da-f]{2})}{chr hex $1}egi;
  $url=decode('utf8',$url);
}

sub add_results {
	my ($self, $query_node, $request_id, $request_guid, $result_file)=@_;

	if ($self->{cfg}{capfilepath}) {
		my($n,$p,$x)=fileparse($result_file);
		$result_file = "$self->{cfg}{capfilepath}/$n$x";
	}

	open(my $rh, "<:raw", $result_file) or do {
		#warn "cannot open $result_file:$!" ;
		return();
		};
  read $rh, my $data, -s $rh;
  close $rh;
  unless ($data =~ /^\s*<!doctype/i) {
    return()
  }

	$data=Encode::decode($cap_encoding, $data);
	my $order=0;
  my $clear;

	if ($self->{cfg}{use_twig}) {
		my $twig = XML::Twig->new(pretty_print=>'indented', escape_gt=>1);
		my $parsed=$twig->safe_parse_html($data);    # build it
		if ($@) {
			warn "failed to parse $result_file: $@";
			return ();
		}
		eval {
			my @sr =$parsed->get_xpath('//div[@id="center_col"]//div[@class="srg"]//div[@class="g"]');
			my $i=0;
			return () if !@sr;
			for my $sr ( @sr) {
				my @hrefs = $sr->get_xpath('.//div[@class="rc"]/[@class="r"]/a') ;
				my ($url, $title ,$text);
				for my $href(@hrefs) {
					$url = $href->att('href');
				}
				my @spans = $sr->get_xpath('.//div[@class="rc"]/div[@class="s"]//span[@class="st"]') ;
				for my $span(@spans) {
					$text= $span->xml_text;
				}
        if (!$clear++) {
          my $req = $query_node->get_child($request_id);
          if ($req->{data}{results}) {
            $self->delete_results($query_node, $request_id);
          }
        }
				my $res_index = $self->add_result('fiddler', $query_node, $request_id, $request_guid, $order++, $url, $title, $text);
			}
		};
		if ($@) {
			warn "error while parsing $result_file: $@";
		}
	}
	else {
		return () unless my ($srg)=$data=~m{(<div\s+class="srg"(?&divs)</div>)$rdef};
		my @g;
		while($srg=~m{(?'cap'<div\s+class="g"(?&divs)</div>)$rdef}g) {
			push @g, $1
		}

		for my $g (@g) {
			my ($rc)=$g=~m{(<div\s+class="rc"(?&divs)</div>)$rdef};
			my ($s)=$g=~m{(<div\s+class="s"(?&divs)</div>)$rdef};
			my ($url,$title)=$rc=~m{<a href="([^"]*)"[^>]*>([^<]*)}si;
			my ($text)=$s=~m{<span class="st"[^>]*>((?&spans))</span>$spandef}si;
			$text=~s{<(\w+)>([^<]*)</\1>}{/ $2 /}sg;

      if (!$clear++) {
        my $req = $query_node->get_child($request_id);
        if ($req->{data}{results}) {
          $self->delete_results($query_node, $request_id);
        }
      }
			my $res_index = $self->add_result('fiddler', $query_node, $request_id, $request_guid, $order++, $url, $title, $text);
			#if (!keys %$results) {
				#warn "no results";
			#}

			#my $res_index = add_result('fiddler', $query, $query_guid, $order++, $url, $title, $text);
			#$results{$url}{guids}{$guid}=[$i++, $title, $text];
			#printf "=======\nurl:%s lbl:%s\n%s\n", $url, $lbl, $text;
		}
	}
}

sub normalize_word {
  my ($self,$word)=@_;
  if ($word=~/^[а-я]/i) {
    my ($root,$pfx,$sfx,$fpfx,$fsfx)=kmg_stats::match_ru($word);
    if (defined $root) {
      $word= "$root$fpfx$fsfx";
    }
    elsif (length $word > 3) {
      my @stems;
      @stems=$stem_ru->stem([$word]);
      $word=shift (@stems)||$_;
    }
  }
  else {
    my ($root,$pfx,$sfx,$fpfx,$fsfx)=kmg_stats::match_en($word);
    if (defined $root) {
      $word= "$root$fpfx$fsfx";
    }
    elsif (length $word > 3) {
      my @stems;
      @stems=$stem_en->stem([$word]);
      $word=shift (@stems)||$_;
    }
  }
  return $word;
}

sub compute_relevance {
  my ($self, $results, $args)=@_;
  my ($wordrx, $stoprx)= ($args->{wordrx},$args->{stoprx});
  my ($cs)=$args->{case_sensitive};
  $wordrx//=qr{\w+};
  $stoprx//=qr{(?!)};
  my $word_docs={};
  my $doc_words={};
  my $doc_word_cnt={};
  my $doc_word_max={};
  #my $results={};
  my $stats={};
  my $i=0;
  my @v = @$results;
  for (my $i=0;$i<@v ;$i++ ) {
    my $res=$v[$i];
    my ($request, $node, $url, $title, $text) = @$res;

    #$text=~s{<[^>]*>}{}g;
    #$title=~s{<[^>]*>}{}g;
    my $t = ${title}. qq'\x00'.${text};
    $t=~s{<[^>]*>|&\w+;|&\#\d+;}{}g;
    while($t=~m{($wordrx)}g) {
			my $word = $cs ? $1 :lc$1;
      next if $word =~ $kmg_stats::stoprx;
      next if $word =~ $kmg_stats::stopmiscrx;

      $word = normalize_word($self,$word);

      if ($word =~ $stoprx) {
        next;
      }

      my $match=1;
      $word_docs->{$word}{$i}++; ## docs per word
      $doc_words->{$i}{$word}++; ## words per doc
      $doc_word_cnt->{$i}++;
      $doc_word_max->{$i} =List::Util::max($doc_word_max->{$i}, $doc_words->{$i}{$word});
    }
  }

  #for (my $i=0;$i<@v ;$i++ ) {
  #    $word_tf3->{$ = $tf3=$doc_words->{$i}{$word}/$doc_word_max->{$i}; # CAN: be precomputed
  #    $avg += $tf3;
  #  }
  for my $word(keys %{$word_docs}) {
    my $wr = $word_docs->{$word};
    my $freq = (keys %$wr) / @v;  ## DF
    my $idf = - log ($freq); ## IDF
    #my $avg;
    my @p ;
    my ($mx,$imx,$textmx);
    for (my $i=0;$i<@v ;$i++ ) {
      next if $doc_word_max->{$i}==0;
      my $tf3=$doc_words->{$i}{$word}/$doc_word_max->{$i}; # CAN: be precomputed
      push @p, $tf3;
      #$avg += $tf3;
      if (!defined $mx || $mx < $tf3) {
        $mx=$tf3;
        $imx=$i;
        $textmx = $v[$i][2]. qq'\n'. $v[$i][3]. qq'\n'. $v[$i][4];
      }
      #$imx = List::Util::max()
    }
    my $ps = pdl (@p);
    my ($mean,$prms,$median,$min,$max,$adev,$rms) = statsover($ps);
    #$stats->{tfidf0}{$word}=$avg / @v;
    $stats->{tfidf3}{$word}="$mean";
    #$stats->{sample}{$word}=$textmx;
  }
  return $stats;
=pod
my $result = {type=>'result', order=>$order, url=>$url, title=>$title, text=>$text} ;
	$data->{results}{$url}= $result;
=cut
}

sub dumper {
    my $t = Data::Dumper->new(\@_)->Terse(1)->Sortkeys(1)->Maxdepth(1)->Dump();
    if (length($t) > 1500) {
      $t=substr($t,0,1497).'...';
    }
    $t
}

sub d { &dumper };
sub pd { print &dumper };

sub get_results {
  my ($self, $nid, $reqid, $word)=@_;
  my $node = $self->g->get_node_by_id($nid);
  die "node $nid does not exist" if !defined $node;
  my $request = $node->get_child($reqid);
  die "request $reqid does not exist" if !defined $request;
  #my $noderef = ref $node eq 'ARRAY' ? $node : $self->{nodes}[$node];
  #if (!defined $noderef) {
  #  return ();
  #}
  my @out;
  #my $results = $node->{data}{results}{$reqid};
  my $results = $request->{data}{results};
  return \@out if !defined $results;
  for my $res(sort { $a->{order}<=>$b->{order}}   @$results) {
    push @out, [ $node,$request, $res->{url} , $res->{title}, $res->{text} ] ;
  }
  return \@out;
}

=pod
sub get_node_ts {
  my ($self,$node)=@_;
  my $ts;
	my $n=$node;
	for (my $i=0;$i<10;$i++) {
		my $nr = $self->{nodes}->[$n];

		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
			return ();
		}

    if ($ts//=$nr->[0]{ts}) {
      last;
    }

		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			last;
		}
	}
  return $ts;
}

=cut

=pod
sub delete_children {
  my ($self,$node)=@_;
  return unless my $data= $self->get_data($node);
  return unless my $children = $data->{children};
  for (my $i=0; $i<@$children; $i++) {
    $self->delete_node_recursive($children->[$i]);
  }
}

=cut

=pod

sub remove_from_parent {
  my($self,$node)=@_;
  my $parent = $self->{nodes}[$node][0]{parent};
  return if !defined $parent ;
  my $parent_data= $self->get_data($parent) || return;
  return if !(my $ch = $parent_data->{children});
  my @new;
  for (my$i=0;$i<@$ch;$i++) {
    if ($ch->[$i]!=$node) {
      push @new, $ch->[$i]
    }
  }
  if (@new < @$ch) {
    $parent_data->{children} = \@new;
  }
}


=cut


sub delete_node_args {
  my ($self,$args)=@_;
  my $nodes= $self->g->nodes;
  my $op = $args->{operation}//'delete';
  if ($op ne 'delete') {
    die "wrong operation '$op'";
  }
	my $node;
	if ($args->{key}) {
		$node= $self->g->get_node_by_key($args->{key});
	}
	elsif ($args->{id}) {
		$node=$self->g->get_node_by_id($args->{id});
	}
	elsif ($args->{url}) {
		$node=$self->g->get_node_by_key("url:$args->{url}");
	}
  if (!$node) {
    die sprintf "node not found for: %s", dumper($args);
  }
	my $rc= $self->g->delete_node($node->id);
  $self->changed(1);
  return $rc;
}

sub delete_edge_args {
  my ($self,$args)=@_;
  my $nodes= $self->g->nodes;
  my $op = $args->{operation}//'delete_edge';
  if ($op ne 'delete_edge') {
    die "wrong operation '$op'";
  }
	my $edge;
  my $eid;
	if ($args->{edge_id}=~/^e(\d+)$/) {
    $eid=$1;
		$edge= $self->g->get_edge_by_id($eid);
	}
  if (!$edge) {
    die sprintf "edge not found for: %s", dumper($args);
  }
	my $rc= $self->g->delete_edge($eid);
  $self->changed(1);
  return $rc;
}

sub update_final_url {
  my ($self,$args)=@_;
  my $nodes= $self->g->nodes;
  my $op = $args->{operation}//'update_final_url';
  if ($op ne 'update_final_url') {
    die "wrong operation '$op'";
  }
	my $node;
	if ($args->{key}) {
		$node= $self->g->get_node_by_key($args->{key});
	}
	elsif ($args->{id}) {
		$node=$self->g->get_node_by_id($args->{id});
	}
	elsif ($args->{url}) {
		$node=$self->g->get_node_by_key("url:$args->{url}");
	}
  elsif ($args->{committed_url}) {
		$node=$self->g->get_node_by_key("url:$args->{committed_url}");
	}
  if (!$node) {
    die sprintf "node not found for: %s", dumper($args);
  }
  $node->data->{final_url}=$args->{final_url};
  $self->changed(1);
  return 1;
}

sub query_storage {
  my ($self, $args)=@_;
  my $storage = join ' ', sort keys %{$self->{storage}};
  my $selection = join ' ', sort keys %{$self->{selection}};
  my $current = $self->current;
  return { storage=> $storage, selection=>$selection, current=>$current};
}

sub update_node_info {
  my ($self, $args)=@_;

  #my $key = ($args->{key}//="url:$args->{url}");
  #my $key = "url:$args->{url}";
  my $op = $args->{operation}//'update';

  if ($op eq 'delete') {
    $args->{key}//$self->derive_key($args);
    delete_node_args($self,$args);
    return {} ; #$self->get_node_info($args);
  }

  if ($op eq 'delete_edge') {
    delete_edge_args($self,$args);
    return {} ; #$self->get_node_info($args);
  }

  if ($op eq 'none') {
    return {};
  }

  if ($op eq 'update_final_url') {
    $self->update_final_url($args);
    return {};
  }

  my $key = ($args->{key}//$self->derive_key($args));

  if ($op!~/^(update|merge|insert)$/) {
    die "wrong operation: $op";
  }

  my $type =$args->{node_type};
  if ($type eq '') {
    die "node_type not specified: $type";
  }


  my $node = $self->g->get_node_by_key($key);
  if ($op eq 'insert' && $node) {
    die "node '$key' already exists";
  }
  elsif (($op eq 'update' || $op eq 'delete') && !$node    ) {
    die "node '$key' does not exist";
  }



  my $direction =$args->{direction};
  if ($direction!~/^(in|out)$/) {
    die "wrong direction: $direction";
  }

	my $node_id;
  my $pkey = $args->{peer_key};
  my ($peer_node,$peer_data,$peer_type,$edge_type);
  $edge_type=  $args->{edge_type};
  if ($pkey) {
    $peer_node = $self->g->get_node_by_key($pkey) or die "update_node_info: peer_key not found: '$pkey'";
    #$peer_data=  $self->{nodes}[$peer_node][0];
    $peer_data=$peer_node->data;
    $peer_type = $args->{peer_type};
    $edge_type = $args->{edge_type};

    if ($peer_type eq '') {
      die "update_node_info: peer_type not specified for key: $pkey";
    }
    if ($edge_type eq '') {
      die "update_node_info: edge_type not specified for key: $pkey";
    }

    if ($peer_type ne '' && $peer_data->{node_type} ne  $peer_type ) {
      die "update_node_info: wrong peer_type: supplied: '$peer_type', expected: '$peer_data->{node_type}'";
    }
  }

  my $data;

  if (!$node) {
    $data = { ( source=>'browser', ts=>time())
             , ( map { /^(fixed_key|url|query|topic|node_type|rating|comment)$/? ($_, $args->{$_} ): () } keys %$args) };
    if ($args->{key}) {
      $self->derive_node_data($data,$key);
    }
    my $meta = { data=>$data, partition=>$self->current, id=>$self->next_id} ;
		$node= graph_object::node->new($meta);
    $node_id=$self->g->add_node($node);
    #$self->{changed}++;
  }

  $data =  $node->data;

  if ($type ne '' && $data->{node_type} ne  $type ) {
    die "update_node_info: wrong node_type: supplied: '$type', expected: '$data->{node_type}'";
  }

      #my $node= get_node($self,$key) || get_or_add_node($self,$key, {node_type=>'url', url=>$url});
      #inherit($self,$node, $req_node);

  if ($pkey && $peer_type ne '') {
    my $meta = { data=>{ }, partition =>$self->current};
    $meta->{data}{edge_type}=$edge_type if defined $edge_type;
    if ($args ->{direction} eq 'out') { # FIX: link_type undefined
      $meta->{from}=$node->id ;
      $meta->{to}=$peer_node->id ;
    }
    elsif ($args ->{direction} eq 'in') {
      $meta->{from}=$peer_node->id ;
      $meta->{to}=$node->id ;
    }
    if ($meta) {
      my $found = $self->g->find_edges( $meta );
      if (!$found || !$found->[0]) {
  #my ($self,$data,$from,$to,$id, $from_child, $to_child)=@_;
        $meta->{id}=$self->next_id;
        my $edge=graph_object::edge->new($meta);
        if (!defined $self->g->add_edge($edge)) {
          die "cannot add new edge";
        }
      }
    }
  }
  $data->{rating} = $args->{rating} if defined $args->{rating};
  $data->{comment} = $args->{comment} if defined $args->{comment};
  $self->changed(1);
  #return $self->get_node_info($args);
  my $result = $self->get_node_info($args);
  return $result;
}

sub switch_session  {
  my ($self, $args)=@_;
  my $selection = $args->{selection}//'today';
  my @selection;
  while ($selection =~ m{\b(?:(none|default|new|today)|(day-\d+|\w+)(?:\s*=\s*(\w+))?)\b}g ) {
    push @selection, $1 ne '' ? $1 : $3 eq '' ? "$2=rw" : "$2=$3";
  }
  $self->change_selection([@selection],{ current=>$args->{current} } );
  return 1;
}

sub get_node_info {
  my ($self, $args)=@_;
  my $key = $args->{key}// "url:$args->{url}";
  my $url_node = $self->g->get_node_by_key($key);
  if (!$url_node) {
    die "not found: $key";
  }
  my $data= $url_node->data;
  #my $json = $self->get_graph_data ($args);

  my $rv = { %$args,
    rating => $data->{rating},
    comment => encode('utf8', $data->{comment}),
  };
  if ($rv->{node_type} eq 'url' && $rv->{url} eq '') {
    $rv->{url}=$key=~s{^url:}{}r;
  }

  if ($rv->{key} ne '') {
    $rv->{key}=encode('utf8', $rv->{key});
  }
  #if ($rv->{node_type} eq 'query' && $rv->{query} eq '') {
  #  $rv->{query}=$key=~s{^google:}{}r;
  #}
  return $rv;
}



sub write_if_changed {
  my ($self)=@_;
  if ($self->changed) {
    printf STDERR "write_if_changed: writing %d changes to the DB...\n", $self->changed;
    my $r = $self->changed;
    $self->write_db();
    printf STDERR "write_if_changed: done\n", $self->changed;
    return $r;
  }
  elsif (my $changed = grep { $self->{_partition}{$_}{changed} }  keys %{$self->{_partition}}) {
    printf STDERR "write_if_changed: writing %d partition changes to the DB...\n", $changed;
    $self->write_db();
    printf STDERR "write_if_changed: done\n", $changed;
    return $changed;
  }
  else {
    print STDERR "write_if_changed: nothing changed\n";
  }
  return ();
}

sub save_new_elements {
  my ($self, $args)=@_;

  my $els = $args ->{elements};
  my $nodes_to_merge={};
  my $edges_to_insert={};
  for (my $i=0;$i<@$els;$i++) {
    my $el = $els->[$i];
    if (!defined $el -> {source}) {
      my $node_id = $el->{id};
      #delete $el->{id};
      if ($node_id!~/^(n\d+|newnode_\d+)$/) {
        die "unexpected id: $node_id"
      }
      my $node;

      if (defined $el->{key}) {
        if ($el->{key} !~ m{^\w+:.+}) {
          die sprintf "the node key not specified: %s", dumper($el);
        }
        $node= $self->g->get_node_by_key( $el->{key});
      }
      elsif (defined $el->{id}) {
        if ($el->{id}=~m{^n(\d+)}) {
          my $id = $1;
          $node=$self->g->get_node_by_id($id);
        }
      }

      if ($node && $el->{id}=~/^newnode/) {
        die sprintf "the new node with this key already exists: %s", dumper($el);
      }
      elsif (!$node && $el->{id}!~/^newnode/) {
        die sprintf "the referred node does not exist: %s", dumper($el);
      }
      if ($nodes_to_merge->{$el->{id}}) {
        die  sprintf "the node encountered twice: %s", dumper($el);
      }
      $nodes_to_merge->{$el->{id}}={ el=>$el, node=>$node && $node->id } ;
    }
  }

  for (my $i=0;$i<@$els;$i++) {
    my $el = $els->[$i];
    if (defined $el -> {source}) {
      my $edge_id = $el->{id};
      die "unexpected edge id: $edge_id" if $edge_id!~/^(e\d+|newedge_\d+)$/;
      if ($el->{edge_type} eq '') {
        die sprintf "the edge type not specified: %s", dumper($el);
      }

      my $el_from= $nodes_to_merge->{$el->{source}};
      my $el_to = $nodes_to_merge->{$el->{target}};

      if (!$el_from) {
        die sprintf "the edge source is unknown: %s", dumper($el);
      }
      if (!$el_to) {
        die sprintf "the edge target is unknown: %s", dumper($el);
      }
      if ($el->{id}!~/^newedge/) {
        die sprintf "the edge is not new: %s", dumper($el);
      }

      if ($edges_to_insert->{$el->{id}}) {
        die sprintf "the edge encountered twice: %s", dumper($el);
      }
      $edges_to_insert->{$el->{id}} = [$el, $el_from, $el_to] ;
    }
  }

  for my $node_to_merge (sort keys %$nodes_to_merge) {
    my $nmr = $nodes_to_merge->{$node_to_merge};
    my $nr = $nmr->{el};
    my $nid;
    my $data = { map { $_, $nr->{$_} } grep { /^(node_type|rating|comment)$/ } keys %$nr };

    if ($nr->{key}) {
      $self->derive_node_data($data,$nr->{key});
    }
    my $key ;
    eval {
      $key = graph_object::node::derive_key(undef,$data);
    };
    if ($@) {
      warn sprintf "could not derive key for %s\n", dumper($data);
    }
    if (!defined ($nid=$nmr->{node})) {
      $data->{ts}//=time();
      #$nmr->{node} = add_node($self,$data , $nr->{key});
      my $node_type = $nr->{node_type};
      if ($node_type eq '') {
        die sprintf "the node type not specified: %s", dumper($nmr);
      }

      my $node = graph_object::node->new({ data=>$data, partition=>$self->current, id=>$self->next_id});
      if (!defined $self->g->add_node( $node )) {
        die "failed to add the node\n";
      }
      $nmr->{node}=$node->id;
    }
    else {
      my $node= $self->g->get_node_by_id($nid);
      if ($key ne '' && $key ne $node->key) {
        $node->change_key($nr->{key});
      }
      common::hash_extend($node->data, $data);
    }
  }

  for my $edge_key (keys %$edges_to_insert) {
    my $new_edge=  $edges_to_insert->{$edge_key};

    my $meta = { from=>$new_edge->[1]{node}, to=>$new_edge->[2]{node}, id=>$self->next_id
                    , data=>{ edge_type=>$new_edge->[0]{edge_type}}, partition=>$self->current};
    #my $found = $self->g->find_edges( $meta );
    #if (!$found || !$found->[0]) {
    #my ($self,$data,$from,$to,$id, $from_child, $to_child)=@_;

    my $edge=graph_object::edge->new($meta);
    if (!defined $self->g->add_edge($edge)) {
      die "cannot add new edge";
    }
  }

  $self->changed(1);

  return 1;
}

sub derive_node_data {
  my ($self,$data,$key)=@_;
  graph_object::node::derive_data($data,$key);
}


sub get_query_stats{
  my ($self, $args)=@_;
  my ($start_s, $end_s) = @$args{qw (start end)};
  my ($start, $end);
  if ($start_s ne '') {
    $start=common::parsetime($start_s);
    if ($start==0) {
      die "wrong start date '$start_s'\n";
    }
  }

  if ($end_s ne '') {
    $end=common::parsetime($end_s);
    if ($start==0) {
      die "wrong end date '$end_s'\n";
    }
  }

  my ($linked, $out);
	for my $link(@{$self->{links}}) {
		my ($edge_type, $from, $to)=@$link;
		next unless $edge_type =~ m{^(jump[123]|tab_link|nav_link|nav_form_submit|query_idea|topic)$};
		my ($fromroot) = get_root($self,$from) || next;
    my ($toroot) = get_root($self,$to) || next;
    my $from_ts = get_node_ts($self,$from);
    my $to_ts = get_node_ts($self,$to);
    if (defined $from_ts) {
      next if defined $start && $from_ts <$start;
      next if defined $end && $from_ts >= $end;
    }
    if (defined $to_ts) {
      next if defined $start && $to_ts <$start;
      next if defined $end && $to_ts >= $end;
    }

		if ($self->{nodes}->[$fromroot]
        && $self->{nodes}->[$fromroot][0]{node_type} eq 'query'
        && $self->{nodes}->[$fromroot][0]{key} !~ m{^\w+:(google.search.query|www\.ya\.ru)$}
        ) {
      #my $p = $nodes->[$to] && $nodes->[$to][0]{parent};
#      my $url = $nodes->[$p] && $nodes->[$p][0]{url};
      my $url = $self->{nodes}->[$toroot][0]{url} || $self->{nodes}->[$to][0]{parent} && $self->{nodes}->[$self->{nodes}->[$to][0]{parent}][0]{url};
      if ($url) {
        push @{$linked->{$self->{nodes}->[$fromroot][0]{key}}{$edge_type}}, $url
      }
    }
    elsif ($self->{nodes}->[$toroot] && $self->{nodes}->[$toroot][0]{node_type} eq 'query'
           && $self->{nodes}->[$toroot][0]{key} !~ m{^\w+:(google.search.query|www\.ya\.ru)$}
           ) {
      my $query = $self->{nodes}->[$toroot][0]{key};
      push @{$linked->{$self->{nodes}->[$fromroot][0]{key}}{$edge_type}}, $query
    }
	}

	my @qnodes;
	my @linktypes = qw(query_idea tab_link nav_link jump1 jump2 jump3 refjump referer);
	my %linktypeorder  = map { $_, $linktypes[$_] } 0..$#linktypes;
	for my $q (@{$self->{nodes}}) {
		next unless ($q->[0]{node_type} eq  'query'
                 || $linked->{$q->[0]{key}});
		my $data = $q->[0];
		my %qs  = map { $_=>[] } @linktypes;
		if ($linked->{$data->{key}}) {
			%qs = ( %qs, %{$linked->{$data->{key}}} );
		}
		push @qnodes, [$q, \%qs];
	}
	@qnodes = map { $_->[0] } sort {
    @{$b->[1]{query_idea}} <=> @{$a->[1]{query_idea}}
		or @{$b->[1]{jump3}} <=> @{$a->[1]{jump3}}
		or @{$b->[1]{jump1}}+@{$b->[1]{jump2}} <=> @{$a->[1]{jump1}}+@{$a->[1]{jump2}}
		or @{$b->[1]{refjump}} <=> @{$a->[1]{refjump}}
		or @{$b->[1]{referer}} <=> @{$a->[1]{referer}}
 	} @qnodes ;

  for my $q (@qnodes) {
    my $data = $q->[0];
    my $cs = $data->{children};
    my ($mints,$maxts, $count, $res_count, $url);
    my $ts;
=pod
    if (defined ($ts=$data->{ts})) {
      next if defined $start && $ts <$start;
      next if defined $end && $ts >= $end;
    }
=cut
    for my $ch (@$cs) {
      my $udata = $self->{nodes}->[$ch] &&$self->{nodes}->[$ch][0] || next;
=pod
      if (defined ($ts=$udata->{ts})) {
        next if defined $start && $ts <$start;
        next if defined $end && $ts >= $end;
      }
=cut
      my $rqs = $udata->{children};

      $url//=$udata->{url};
      my(@rout);
      $count++;
      for my $req (@$rqs) {
        my $rdata = $self->{nodes}->[$req]&&$self->{nodes}->[$req][0]||next;
				if ($rdata->{results}) {
					$res_count+=0+keys %{$rdata->{results}} ;
					for my $r(sort keys %{$rdata->{results}}) {
						push @rout, $r."\n";
					}
				}

        $mints //= $rdata->{ts};
        if ($mints > $rdata->{ts}) {
          $mints = $rdata->{ts}
        }
        $maxts //= $rdata->{ts};
        if ($maxts < $rdata->{ts}) {
          $maxts = $rdata->{ts}
        }
      }

      #print for (@rout);
    }

    if (defined $mints) {
      next if defined $start && $maxts <= $start;
      next if defined $end && $mints > $end;
    }
    my $qref = $linked->{$data->{key}};
    #my $j = join '' ,map { qq{[$_:$qref->{$_}]} } keys%$qref;
    #next unless $j ne '';
    $out.=sprintf "%s: [%s][%s]:\t%s\n", $data->{key}, $count,  $res_count, $url;
    for my $k (sort keys %$qref) {
      my $ur = $qref->{$k};
      for my $u (@$ur) {
        $out.=sprintf "\t%s:%s\n", $k, $u
      }
    }
  }
	$out;
}


sub parent {
  my ($self, $node);
  my $nr = $self->{nodes}[$node];
  if ($nr && $nr ->[0]{parent}) {
    return $nr ->[0]{parent}
  }
  return ();
}

=pod
$ffun->($node, $noderef, $level)
if $ffun returns 1, recursion stops
=cut


sub get_stats {
  my($self)=@_;
  my $stats={};
  my $seen_nodes;
  for my $eid(keys %{$self->g->edges}) {
    my $edge = $self->g->get_edge_by_id($eid);
    my $from = $self->g->get_node_by_id($edge->from);
    my $ftype = $from->data->{node_type}//'unknown';
    my $to = $self->g->get_node_by_id($edge->to);
    my $ttype = $to->data->{node_type}//'unknown';
    if (!$seen_nodes->{$from->id}++) {
      $stats->{node}++;
      $stats->{source_node}++;
      $stats->{"node_type:$ftype"}++;
    }
    if (!$seen_nodes->{$to->id}++) {
      $stats->{node}++;
      $stats->{target_node}++;
      $stats->{"node_type:$ttype"}++;
    }
    my $etype = $edge->data->{edge_type} // 'unknown';
    $stats->{edge}++;
    $stats->{"edge_type:$etype"}++;
  }

  for my $nid(keys %{$self->g->nodes}) {
    my $from = $self->g->get_node_by_id($nid);
    my $ftype = $from->data->{node_type}//'unknown';
    if (!$seen_nodes->{$from->id}++) {
      $stats->{node}++;
      $stats->{isolated_node}++;
      $stats->{"node_type:$ftype"}++;
    }
  }
  return $stats;
}


sub get_filtered_edges  {
  my ($self, $args)=@_;
  my $filters = $args->{filters};
  my $nodes = $self->g->nodes;
  my $edges = $self->g->edges;

  my $flt_edges = {};
  my $seen={};
  my $peers ={};
  my $flt_nodes = {};

  my ($stats)={};

  for my $filter (@$filters) {
    print STDERR ('get_filtered_edges: '. join '',  map { qq{$_: $filter->{$_}\n} } sort keys %$filter), $/
      if $self->{verbose};

    my $match_undef=$args->{match_undef};
    my ($start_s, $end_s) = @$filter{qw (start end)};
    s{[\r\n]+}{}g for ($start_s, $end_s);
    my ($start, $end);

    if ($start_s ne '') {
      $start=common::parsetime($start_s);
      if ($start==0) {
        die "wrong start date '$start_s'\n";
      }
    }

    if ($end_s ne '') {
      $end=common::parsetime($end_s);
      if ($end==0) {
        die "wrong end date '$end_s'\n";
      }
    }

    my $in_range;

    my $tsflt = sub {
      my ($id,$node,$parent,$root,$level)=@_;
      my $ts;
      if ($node->{data}&& ($ts= $node->{data}{ts})) {
        if ((!defined $start || $ts >=$start) && (!defined $start || $ts >=$start) ) {
          $in_range=1;
          return 1;
        }
      }
      0;
    };

    if ($filter->{filter_type} eq 'edge' || $filter->{type} eq 'edge') {
      my $fltrx = {} ;
      for my $flt_obj (qw (from to edge)) {

        for my $fltkey ( qw (edge_type node_type text key comment rating source )) {
          if (defined (my $t =$filter->{$flt_obj}{$fltkey})) {
            $t =~ s{(?:^|\G)((?>\\.|.)*?)\\Q((?>\\.|.)*?)(?:\\E|$)}{$1.quotemeta($2)}eg;
            $fltrx->{$flt_obj}{$fltkey} = common::make_re($t);
          }
        }
      }
      $fltrx->{edge}//={ edge_type => qr/^(jump[123]?|tab_\w+|nav_\w+|query_idea|session|related)$/ };
      $fltrx->{from}//={ text=>qr/^/ };
      $fltrx->{to}//={ text=>qr/^/ };


      for my $eid (sort keys %$edges) {
        my $edge= $self->g->get_edge_by_id($eid);
        my ($edge_type) = $edge->data->{edge_type};
        my $id_from= $edge->from;
        my $id_to = $edge->to;


        #my @path_from;
        #my @path_to;
        #my $from_traits=get_farthest_ancestors($self, $from, sub {
        #  my ($n,$nr)= @_; push @path_from,$n; grep {$nr->[0]{node_type} eq $_ || $nr->[0]{$_}} qw(topic query url ts);
        #});

        #my $to_traits=get_farthest_ancestors($self, $to, sub {
        #  my ($n,$nr)= @_; push @path_to,$n; grep {$nr->[0]{node_type} eq $_ || $nr->[0]{$_}} qw(topic query url ts);
        #});

        #my $fromnode  = $from_traits->{topic} || $from_traits->{query} || $from_traits->{url};
        #my $tonode = $to_traits->{topic} || $to_traits->{query} || $to_traits->{url};

        my $node_from = $self->g->get_node_by_id($id_from);
        my $node_to = $self->g->get_node_by_id($id_to);
        my $data_from = $node_from->data;
        my $data_to = $node_to->data;
        #my $ts_from= $data->{ts};
        #my $ts_to= $to_traits->{ts} && $nodes->[$to_traits->{ts}][0]{ts};

        if (!$data_from) {
          die sprintf "node %s: no data ", $edge->from;
        }
        if (!$data_to) {
          die sprintf "node %s: no data ", $edge->to;
        }
        #next unless ($fromnode && $tonode);

        $in_range=undef;
        $node_from->descend_dl(undef, $node_from, $tsflt, 0);
        my $from_in_range =$in_range;
        $in_range=undef;
        $node_to->descend_dl(undef, $node_to, $tsflt, 0);
        my $to_in_range =$in_range;

        unless  ($from_in_range || $to_in_range) {
          next;
        }


        my $t;
        next unless (defined ($t= $fltrx->{edge}{edge_type}) && $edge_type =~ $t);

        #next unless my $data_from = $self->get_data($from);
        #next unless my $data_to= $self->get_data($to);
        #next unless my $data_fromnode = $self->get_data($fromnode);
        #next unless my $data_tonode= $self->get_data($tonode);

        my ($from_matched,$to_matched);
        for my $rxkey ( qw(node_type key text comment rating source) ) {
          if (defined $fltrx->{from}{$rxkey}) {
            my @fields = @{$rxkey_to_field->{$rxkey}};
            my $rx_matched;
            for my $field(@fields) {
              my $value;
              if ($field eq 'key') {
                $value = $self->g->get_key_by_id($node_from->id);
              }
              else {
                $value = $data_from->{$field};
              }
              if (defined $value) {
                if ($value=~$fltrx->{from}{$rxkey}) {
                  $rx_matched=1; last ;
                }
                $rx_matched=0;
              }
            }
            #last if defined $rx_matched;
            $from_matched=(defined $rx_matched ? $rx_matched : $match_undef );
            last if $from_matched==0;
          }
        }

        for my $rxkey ( qw(node_type key text comment rating source) ) {
          if (defined $fltrx->{to}{$rxkey}) {
            my @fields = @{$rxkey_to_field->{$rxkey}};
            my $rx_matched;
            for my $field(@fields) {
              my $value;
              if ($field eq 'key') {
                $value = $self->g->get_key_by_id($node_to->id);
              }
              else {
                $value = $data_to->{$field};
              }
              if (defined $value) {
                if ($value=~$fltrx->{to}{$rxkey}) {
                  $rx_matched=1; last ;
                }
                $rx_matched=0;
              }
            }
            #last if defined $rx_matched;
            $to_matched=(defined $rx_matched ? $rx_matched : $match_undef );
            last if $to_matched==0;
          }
        }

        next unless 1==($to_matched // $match_undef)  && 1 ==($from_matched // $match_undef);

        $stats->{filtered_edges}++;
        #$flt_edges->{$i}=[$i,$fromnode, $tonode];
        $flt_edges->{$eid}=[$eid, $node_from->id, $node_to->id];

        if (!$seen->{$node_from->id}++) {
          $peers->{$node_from->id}++;
          $stats->{filtered_nodes}++;
        }
        if (!$seen->{$node_to->id}++) {
          $peers->{$node_to->id}++;
          $stats->{filtered_nodes}++;
        }
        #$seen->{$from}++;
        #$seen->{$to}++;


        my $mx = ($args->{max_edges} // $self->{cfg}->{max_edges} //  3000 );
        if ($mx && $stats->{filtered_edges} >  $mx) {
          print STDERR "get_filtered_edges: hit the limit of max $mx edges\n";
          last
        }
      }
    }
    elsif ($filter->{filter_type} eq 'node' || $filter->{filter_type} eq 'single_node') {
      my $has_edges={};

      my $fltrx = {} ;
      for my $flt_obj (qw (from)) {
        for my $fltkey ( qw (node_type text key source )) {
          if (defined (my $t =$filter->{$flt_obj}{$fltkey})) {
            $t =~ s{(?:^|\G)((?>\\.|.)*?)\\Q((?>\\.|.)*?)(?:\\E|$)}{$1.quotemeta($2)}eg;
            $fltrx->{$flt_obj}{$fltkey} = common::make_re($t);
          }
        }
      }
      $fltrx->{from}//={ text=>qr/^/ };

      for my $nid ( sort keys %$nodes) {
        my $node_from = $self->g->get_node_by_id($nid);
        if ($filter->{filter_type} eq 'single_node') {
          next if !!keys %{$node_from->edges};
          #my $root = $self->get_root($i);
          #next if $has_edges->{$root}//=$self->has_edges($root);
        }
        #my @path_from;
        #my $from_traits=get_farthest_ancestors($self, $i, sub {
        #  my ($n,$nr)= @_; push @path_from, $n;grep {$nr->[0]{node_type} eq $_ || $nr->[0]{$_}} qw(query url ts);
        #});

        #my $fromnode= $from_traits->{query}||$from_traits->{url};
        #next if !$fromnode || $seen->{$fromnode};
        #my $from_ts = $from_traits->{ts};
        #my ($from_in);

        my $data_from = $node_from->data;

        if (!$data_from) {
          die sprintf "node %s: no data ", $nid;
        }

        $in_range=undef;
        $node_from->descend_dl(undef, $node_from, $tsflt, 0);
        unless  ($in_range) {
          next;
        }

        my ($from_matched,$to_matched);
        for my $rxkey ( qw( node_type key text comment rating source) ) {
          if (defined $fltrx->{from}{$rxkey}) {
            my @fields = @{$rxkey_to_field->{$rxkey}};
            my $rx_matched;
            for my $field(@fields) {
              my $value;
              if ($field eq 'key') {
                $value = $self->g->get_key_by_id($node_from->id);
              }
              else {
                $value = $data_from->{$field};
              }
              if (defined $value) {
                if ($value=~$fltrx->{from}{$rxkey}) {
                  $rx_matched=1; last ;
                }
                $rx_matched=0;
              }
            }
            #last if defined $rx_matched;
            $from_matched=(defined $rx_matched ? $rx_matched : $match_undef );
            last if $from_matched==0;
          }
        }

        next unless 1 ==($from_matched // $match_undef);

        $flt_nodes->{$node_from->id}++;
        if (!$seen->{$node_from->id}++) {
          if ($filter->{filter_type} eq 'single_node') {
            $stats->{single_nodes}++;
          }
          $stats->{filtered_nodes}++;
        }

        my $mx = ($args->{max_nodes} //$self->{cfg}->{max_nodes} //3000 );
        if ($mx && keys %$flt_nodes >  $mx) {
          print STDERR "get_filtered_edges: hit the limit of max $mx nodes\n";
          last
        }
      }
    }
  }

  printf STDERR "get_filtered_edges: edges=%s, nodes=%s, single nodes=%s, notable: %s\n"
  , $stats->{filtered_edges}, $stats->{filtered_nodes}, $stats->{single_nodes}, $stats->{notable}
    if $self->{verbose};

  return { nodes=>$flt_nodes, edges=>$flt_edges, peers=>$peers, stats=>$stats};
}


sub get_graph_data  {
  my ($self, $args)=@_;
  my $data = get_graph_data_with_stats ($self, $args);
  my $stats = $data->{stats};
  if ($self->{verbose}) {
    print STDERR "get_graph_data:\n";
    for (sort keys %$stats)  {
      printf STDERR "\t%s: %s\n", $_,  $stats->{$_}
    }
  }
  return $data->{cy_elements};
}

sub get_query_relevance {
  my ($self, $node, $args)=@_;
  $args//={};
  my $results=[];
  my $seen={};
  my $dups={};
  my $stats = $node->data->{relevance};
  if (!$args->{recompute} && $stats && $stats->{tfidf3} && !!keys %{$stats->{tfidf3}} ) {
    return $stats;
  }
  $node->descend_dl(undef, $node, sub {
    my ($id,$child,$parent,$root,$level)=@_;
    my $out = $self->get_results($root->id,$child->{id});
    if ($seen->{$root->id}{$child->{id}}++) {
      #printf STDERR "duplicate results: %s %s\n", $root->id, $child->{id};
      return ();
    }
    return () if !defined $out;
    push @$results, @$out;
    0;
  });

  #printf "======= request %s.%s =======\n",  $root->id, $child->{id};
  #my $stop = qr{\b(cytoscape|js|layout|text|nodes?)\b}i;
  #printf STDERR "total results for node %s: %s\n", $node->id, 0+@$results;
  $stats=$self->compute_relevance($results,
                                     { wordrx=>$args->{wordrx}, stoprx=>$args->{stoprx},
                                      case_sensitive=>$args->{case_sensitive}});
  $node->data->{relevance}=$stats;
  if (defined (my $t = $node->partition)) {
    $self->changed(1);
  }
  return $stats;
}

sub get_query_stopwords {
  my ($self,$node)=@_;
  my $query = $node->data->{query};
  my $key = $node->key;
  if (!defined $query && defined $key) {
    ($query) = $key =~m{^\w+:(.*)};
  }
  if (!defined $query) {
    return ();
  }
  my @stop = $query =~ m{(\w+)}g;
  for (my $i =0 ;$i<@stop;$i++) {
    $stop[$i] = normalize_word($self,$stop[$i]);
    #if ($word=~/^[а-я]/i) {
    #  $stop[$i] = ($stem_ru->stem([$word]))[0];
    #}
    #else {
    #  $stop[$i] = ($stem_en->stem([$word]))[0];
    #}
  }
  my ($stoprx) ;
  if (@stop) {
    ($stoprx) = map {qr{\b($_)\b}i } join '|', @stop ;
  }
  return $stoprx;
}

sub cy_query_relevance {
  my ($self, $stats, $cnt)=@_;
  my $out;
  my $i =0;
  for my $word (sort {$stats->{tfidf3}{$b} <=> $stats->{tfidf3}{$a} } keys %{$stats->{tfidf3}}) {
    last if $cnt>0 && $i++ >=$cnt;
    $out .= sprintf "%s: %.2f\n" , $word , $stats->{tfidf3}{$word};
  }
  $out;
}



sub get_expanded_edges {
  my ($self, $args)=@_;
  my $nodes = $self->g->nodes;
  my $edges = $self->g->edges;

  my ($stats)={};

  $args->{match_undef}//=0;
  $args->{expand}//=1;

  my $result = $self->get_filtered_edges($args);
	#$flt_edges->{$eid}=[$eid, $node_from, $node_to];

  my $filtered_edges =$result->{edges};
  my $filtered_peers = $result->{peers};
  my $filtered_nodes =$result->{nodes};
  $stats = $result->{stats};

  my ($expanded_edges,$flattened_edges);
  if ($args->{expand}) {
    my $linked_nodes  = { map { $_->[1], 1 } values %$filtered_edges };

    $expanded_edges = $self->g->get_subtree($linked_nodes);
    my $exp_stats = $self->g->get_stats($expanded_edges);
    $stats -> {expanded_nodes} += $exp_stats->{nodes};
    $stats -> {expanded_edges} += $exp_stats->{edges};
  }
  else {
    $expanded_edges = $filtered_edges;
  }

  $stats->{filtered_edges}//=0;
  return ($expanded_edges, $stats, $filtered_nodes, $filtered_edges, $filtered_peers);
}

sub filtered_subgraph {
  my ($self,$args)=@_;
  my ($x_edges, $stats, $x_nodes)=get_expanded_edges($self,$args);
  my ($ndetach, $edetach);

  for my $eid (keys %$x_edges) {
    my $edge=$self->g->get_edge_by_id($eid);
    my $rootfrom = $self->g->get_node_by_id($edge->from);
    my $rootto = $self->g->get_node_by_id($edge->to);
    $ndetach->{$edge->from}++;
    $ndetach->{$edge->to}++;
    $edetach->{$eid}++;
  }
  for my $nid (keys %$x_nodes) {
    $ndetach->{$nid}++;
  }
  return ($ndetach, $edetach);
}


=pod
sub assign_partition {
  my ($self, $args)=@_;
  my $partition=$args->{partition};
  my $clean = $args->{clean}//0;
  if (!defined $partition) {
    die "partition not defined"
  }
  my ($ndetach, $edetach) = $self->filtered_subgraph($args);

  my $changed = $self->g->assign_partition($partition,1,$ndetach,$edetach);
  if (!!keys %$changed) {
    $self->{changed}=1;
    for my $c (keys %$changed){
      $self->{_selection}{$c}{changed}=1;
    }
  }
}
=cut

sub copy_subgraph {
  my ($self, $dest, $args)=@_;
  if ($dest eq '') {
    die "destination is empty" ;
  }
  my ($ndetach, $edetach) = $self->filtered_subgraph($args);
  if ($args->{separate}) {
    my ($nleft,$eleft) = $self->g->pivot($ndetach, $edetach);
    my ($n_i_sect) = $self->g->intersect($ndetach, $self->{g}{nodes});
    my ($e_i_sect) = $self->g->intersect($edetach, $self->{g}{edges});
    my ($ndetachminus) = $self->g->minus($ndetach,$n_i_sect);
    my ($edetachminus) = $self->g->minus($edetach,$e_i_sect);
    $self->g->assign_partition($dest,1,$ndetachminus,$edetachminus);
    $self->g->assign_partition($dest,0,$n_i_sect,$e_i_sect);
    $self->changed(1);
    #$self->{storage}{$dest}{file}//=$self->get_partition_file($dest);
    $self->{selection}{$dest}{mode}//='rw';#$self->get_partition_file($dest);
  }
  else {
    my $detached = $self->g->db_subset($ndetach,$edetach);
    $detached->assign_partition($dest,1);
    $self->save_subgraph($dest,$detached); # might need $self->changed(1);
    if ($args->{detach}) {
      my ($nleft,$eleft) = $self->g->pivot($ndetach, $edetach);
      my $g_left = $self->g->db_subset($nleft,$eleft);
      #$g_left->assign_partition($dest,0,$ndetach,$edetach);
      $self->g($g_left);
      $self->changed(1);
    }
  }
  if ($args->{savedb}) {
    $self->write_db();
  }
}

sub separate_subgraph {
  my ($self, $dest, $args)=@_;
  my ($ndetach, $edetach) = $self->filtered_subgraph($args);
  my $detached = $self->g->db_subset($ndetach,$edetach);
  if ($dest eq '') {
    die "destination is empty" ;
  }
  $detached->assign_partition($dest,0);
  $self->save_subgraph($dest,$detached); # might need $self->changed(1);
  if ($args->{detach}) {
    my ($nleft,$eleft) = $self->g->pivot($ndetach, $edetach);
    my $g_left = $self->g->db_subset($nleft,$eleft);
    $g_left->assign_partition($dest,0,$ndetach,$edetach);
    $self->g($g_left);
    $self->changed(1);
  }
}


sub save_subgraph {
  my ($self, $dest, $detached)=@_;
  $self->write_partition($detached,$dest);
  #$self->{storage}{$dest}{file}=$self->get_partition_file($dest);
}


sub get_graph_data_with_stats {
  my ($self, $args)=@_;
  #my ($filters) = $args->{ filters } ;
  #my ($filters) = $json->decode($args->{ filters });
  my $nodes = $self->g->nodes;
  my $edges = $self->g->edges;

  my $cyj={elements=>[]};

  my ($expanded_edges, $stats, $filtered_nodes, $filtered_edges, $filtered_peers)= $self->get_expanded_edges($args);
  #my ($stats)={};

  #$args->{match_undef}//=0;
  #$args->{expand}//=1;

  #my $result = $self->get_filtered_edges($args);
	#$flt_edges->{$eid}=[$eid, $node_from, $node_to];

  #my $filtered_edges =$result->{edges};
  #my $filtered_peers = $result->{peers};
  #my $filtered_nodes =$result->{nodes};
  #$stats = $result->{stats};
  #$stats -> {filtered_nodes} = $flt_stats->{filtered_nodes};
  #$stats -> {filtered_edges} = $flt_stats->{filtered_edges};

  #my ($expanded_edges,$flattened_edges);
  #if ($args->{expand}) {
  #  my $linked_nodes  = { map { $_->[1], 1 } values %$filtered_edges };
  #
  #  $expanded_edges = $self->g->get_subtree($linked_nodes);
  #  my $exp_stats = $self->g->get_stats($expanded_edges);
  #  $stats -> {expanded_nodes} += $exp_stats->{nodes};
  #  $stats -> {expanded_edges} += $exp_stats->{edges};
  #}
  #else {
  #  $expanded_edges = $filtered_edges;
  #}

  #$stats->{filtered_edges}//=0;

  my ($linked, $out);
  my $added_nodes={};
  #for (my $i=0;$i<@$filtered_edges;$i++) {
  #my @flt_edges = sort keys %$expanded_edges
  my $notable={};
  #for my $key (sort { $a <=> $b } keys %$expanded_edges) {
  my $wordrx = $args->{word} ? qr/$args->{word}/i : qr{\w+};
  my $stoprx = $args->{stop} ? qr/$args->{stop}/i : undef;

  my $relstats={};
  my $comprel_stats={};

  for my $eid (sort { $a <=> $b } keys %$expanded_edges) {
    my $edge = $self->g->get_edge_by_id($eid);
    my $mx  = $args->{max_cy_elements} ||  $self->{cfg}->{max_cy_elements} ||  5000 ;
    if (@{$cyj->{elements}}> ($mx)) {
      print STDERR "get_graph_data_with_stats: hit the limit of max $mx cy elements\n";
      last;
    }
    my ( $edge_type) = ( $edge->data->{edge_type});
		my $fnid =$edge->from;
		my $tnid =$edge->to;
    my $classes;
    my $relevance;
    if (!$added_nodes->{$fnid}) {
      my $node_from =$self->g->get_node_by_id($fnid);
      my $data_from= $node_from->data;
      $classes= { };

      if ( $data_from && ( defined($data_from->{rating}) || defined($data_from->{comment}))) {
        $classes->{notable}++;
        if (!$notable->{$fnid}++) {
          $stats->{notable}++;
        }
      }
      if ($filtered_nodes->{$fnid} || $filtered_peers->{$fnid}) {
        $classes->{filtered}++;
      }
      my $node_type = $data_from->{node_type};
      my $data = { classes=>$classes };
      my $relstats;
      if ($node_type eq 'query') {
        if (!defined $relstats->{$node_from->{id}}) {
          my $qstoprx = $stoprx // get_query_stopwords($self, $node_from);
          $comprel_stats->{called}++;
          my $rel_args = {wordrx=>$wordrx, stoprx=>$qstoprx};
          if ($args->{compute_relevance}) {
            $rel_args->{recompute}=1
          }
          $relstats->{$node_from->{id}}=$self->get_query_relevance($node_from, $rel_args);
        }
        $data->{relevance} = cy_query_relevance($self, $relstats->{$node_from->{id}}, $args->{relevance_count}//15);
      }
      $self->add_cy_node($cyj, $fnid, $data);
      $stats->{"node type:$node_type"}++;
    }

    if (!$added_nodes->{$tnid}) {
      $classes = {};
      my $node_to =$self->g->get_node_by_id($tnid);
      my $data_to= $node_to->data;
      if ( $data_to && ( defined($data_to->{rating}) || defined($data_to->{comment}))) {
        $classes->{notable}++;
        if (!$notable->{$tnid}++) {
          $stats->{notable}++;
        }
      }
      if ($filtered_nodes->{$tnid}  || $filtered_peers->{$tnid} ) {
        $classes->{filtered}++;
      }
      my $node_type = $data_to->{node_type};
      my $data = { classes=>$classes };
      my $relstats;
      if ($node_type eq 'query') {

        if (!defined $relstats->{$node_to->{id}}) {
          my $qstoprx = $stoprx // get_query_stopwords($self, $node_to);
          $comprel_stats->{called}++;
          my $rel_args = {wordrx=>$wordrx, stoprx=>$qstoprx};
          if ($args->{compute_relevance}) {
            $rel_args->{recompute}=1
          }
          $relstats->{$node_to->{id}}=$self->get_query_relevance($node_to, $rel_args);
        }
        $data->{relevance} = cy_query_relevance($self, $relstats->{$node_to->{id}}, $args->{relevance_count}//15);

        # remove after commits: 3
        #my $qstoprx = $stoprx // get_query_stopwords($self, $node_to);
        #if (!defined $relstats->{$node_to->{id}}) {
        #  $comprel_stats->{called}++;
        #  $relstats->{$node_to->{id}}=$self->get_query_relevance($node_to, {wordrx=>$wordrx, stoprx=>$qstoprx});
        #}
        #
        #$relstats->{$node_to->{id}} = $self->get_query_relevance($node_to, {wordrx=>$wordrx, stoprx=>$stoprx});
        #
        #$data->{relevance} = cy_query_relevance($self, $relstats->{$node_to->{id}}, $args->{cnt}//5);
      }

      $self->add_cy_node($cyj, $tnid, $data);

      $stats->{"node type:$node_type"}++;
    }
    $self->add_cy_edge($cyj, $eid, $fnid, $tnid, { classes=> { $filtered_edges->{$eid} ? qw(filtered 1):()  } } );
    $stats->{"edge type:$edge_type"}++;
  }

  #for (my $i=0;$i<@$filtered_nodes;$i++) {
  for my $nid ( keys %$filtered_nodes) {
  #for (my $i=0;$i<@{$self->{links}};$i++) {
    my $mx  = $args->{max_cy_elements} ||  $self->{cfg}->{max_cy_elements} ||  5000 ;
    if (@{$cyj->{elements}}> ($mx)) {
      print STDERR "get_graph_data_with_stats: hit the limit of max $mx cy elements\n";
      last;
    }
    my $fnid  = $nid;
    #my $fnid  = $filtered_nodes->{$key};
    #my ($fnid) = @{$filtered_nodes->[$i]};

    my $classes;
    if (!$added_nodes->{$fnid}) {
      my $node_from =$self->g->get_node_by_id($fnid);
      my $data_from= $node_from->data;
      $classes= { };

      if ( $data_from && ( defined($data_from->{rating}) || defined($data_from->{comment}))) {
        $classes->{notable}++;
        if (!$notable->{$fnid}++) {
          $stats->{notable}++;
        }
      }
      if ($filtered_nodes->{$fnid} || $filtered_peers->{$fnid}) {
        $classes->{filtered}++;
      }
      my $node_type = $data_from->{node_type};
      my $data = { classes=>$classes };
      my $relstats;
      if ($node_type eq 'query') {

        if (!defined $relstats->{$node_from->{id}}) {
          my $qstoprx = $stoprx // get_query_stopwords($self, $node_from);
          $comprel_stats->{called}++;
          my $rel_args = {wordrx=>$wordrx, stoprx=>$qstoprx};
          if ($args->{compute_relevance}) {
            $rel_args->{recompute}=1
          }
          $relstats->{$node_from->{id}}=$self->get_query_relevance($node_from, $rel_args);
        }
        $data->{relevance} = cy_query_relevance($self, $relstats->{$node_from->{id}}, $args->{relevance_count}//15);

        #my $qstoprx = $stoprx // get_query_stopwords($self, $node_from);
        #
        #if ($args->{compute_relevance}) {
        #  if (!defined $relstats->{$node_from->{id}}) {
        #    $comprel_stats->{called}++;
        #    $relstats->{$node_from->{id}}=$self->get_query_relevance($node_from, {wordrx=>$wordrx, stoprx=>$qstoprx});
        #  }
        #  $data->{relevance} = cy_query_relevance($self, $relstats->{$node_from->{id}}, $args->{cnt}//15);
        #}
      }
      $self->add_cy_node($cyj, $fnid, $data);
      $stats->{"node type:$node_type"}++;
    }

    #$self->add_cy_node($cyj, $fnid, { classes=> { $filtered_nodes->{$fnid} ? qw(filtered 1):()  } } );
    #my $node= $self->g->get_node_by_id($fnid);
    #my $data_from = $node->data;
    #my $node_type = $data_from->{node_type};
    #$stats->{"node type:$node_type"}++;

    #$self->add_cy_json ($cyj, undef, $fnid, undef);
    #$stats->{filtered_nodes}++;

  }

  printf STDERR "compute_relevance stats: %s\n", dumper($comprel_stats);

  $self->{requests}++;
  return { stats=>$stats, cy_elements=>$cyj->{elements} };
}

sub add_cy_node  {
    my ($self, $cyj, $from, $traits) =@_;
    $traits //={};
    return () if (ref $cyj ne 'HASH');
    my $nodes = $self->g->nodes;
    #my $links = $self->{links};

    $cyj->{elements}//=[];
    $cyj->{nodes}//={};
    my $fromnode= $self->g->get_node_by_id($from);
    my $fromdata=$fromnode->data;
    my $fromkey = $fromnode->key;
    my $url = $fromdata->{url};
    my $final_url = $fromdata->{final_url};

    if ($fromnode) {
      $cyj->{nodes}{$from} //=@{$cyj->{elements}};
      my $t = { group=> 'nodes',
          data => {
            id => "n$from" ,
            name => $fromkey
            , node_type=>$fromdata->{node_type}
            , key => $fromkey
            , defined $url ? (url=>$url):()
            , defined $final_url ? (final_url=>$final_url):()
            , rating => $fromdata->{rating}
            , comment => $fromdata->{comment}
            , relevance => $traits->{relevance}
            #, notable => $from_notable
          },
          classes => $traits->{classes} ? join (' ', keys %{$traits->{classes} }) : ""
          };
      #if (keys %{$traits->{classes}}) {
      #  $t->{classes}=join (' ', keys %{$traits->{classes} });
      #}
      my ($start,$end) = $self->node_ts($fromnode);
      my $tss = common::ts($start);
      my $tse = common::ts($end);
      $t->{data}{ts} = $tss if $start;
      $cyj->{elements}[$cyj->{nodes}{$from}]=$t;
    }


    return $cyj;
}

sub old_add_cy_node  {
    my ($self, $cyj, $from, $traits) =@_;
    $traits //={};
    return () if (ref $cyj ne 'HASH');
    my $nodes = $self->{nodes};
    #my $links = $self->{links};

    $cyj->{elements}//=[];
    $cyj->{nodes}//={};
    my $fromnode= $nodes->[$from];
    #my $tonode = $nodes->[$to];
    #my (@from_classes) ;
    #my (@from_classes,@to_classes) ;
    #push @from_classes, $traits->{from_class} if ref $traits eq 'HASH' && defined $traits->{from_class} ;
    #push @to_classes, $traits->{to_class} if ref $traits eq 'HASH' && defined $traits->{to_class} ;
    #my $from_notable = $fromnode && defined($self->{nodes}->[$from][0]{rating})||defined($self->{nodes}->[$from][0]{comment});
    #my $to_notable = $tonode &&  defined($self->{nodes}->[$to][0]{rating})||defined($self->{nodes}->[$to][0]{comment});
    #if ($from_notable) {
    #  push @from_classes , 'notable';
    #}
    #if ($to_notable) {
    #  push @to_classes , 'notable';
    #}

    if ($fromnode) {
      $cyj->{nodes}{$from} //=@{$cyj->{elements}};
      my $t = { group=> 'nodes',
          data => {
            id => "n$from" ,
            name => $self->{nodes}->[$from][0]{key}
            , node_type=>$self->{nodes}->[$from][0]{node_type}
            , key => $self->{nodes}->[$from][0]{key}
            , rating => $self->{nodes}->[$from][0]{rating}
            , comment => $self->{nodes}->[$from][0]{comment}
            #, notable => $from_notable
          },
          classes => $traits->{classes} ? join (' ', keys %{$traits->{classes} }) : ""
          };
      #if (keys %{$traits->{classes}}) {
      #  $t->{classes}=join (' ', keys %{$traits->{classes} });
      #}

      $cyj->{elements}[$cyj->{nodes}{$from}]=$t;

    }


    return $cyj;
}

sub add_cy_edge  {
    my ($self, $cyj, $eid, $fnid, $tnid, $traits) =@_;
    $traits//={};
    return () if (ref $cyj ne 'HASH');
    my $nodes = $self->g->nodes;
    my $links = $self->g->edges;
    my $edge= $self->g->get_edge_by_id($eid);
    my $edata = $edge->data();

    $cyj->{elements}//=[];
    push @{$cyj->{elements}}, {
          group => 'edges'
          , data => {
              id=>"e$eid",
              source=>"n$fnid",
              target=>"n$tnid",
              edge_type =>$edata->{edge_type}
            }
          , classes => $traits->{classes} ? join (' ', keys %{$traits->{classes} }) : ""
          };
    return $cyj;
}

sub print_query_stats {
  print &get_query_stats
}


sub get_closest_ancestors  {
	my ($self, $node, $ffun)=@_;
  #my $trait_count = keys $traits;
	my $n=$node;
  my $traits ={};
  my $trait_nodes={};

	for (my $i=0;$i<100;$i++) { # TODO: 10 -> cfg parameter
		my $nr = $self->{nodes}->[$n];
    my @node_traits = $ffun->($n,$nr) ;
    for my $nt (@node_traits) {
      if (!$traits->{$nt}++) {
        $trait_nodes->{$nt}=$n
      }
    }
		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
      die sprintf "%s: node[$n] inconsistent: %s", (caller 0)[3], dumper($nr);
			#return ();
		}
		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			last;
		}
	}
  return $trait_nodes;
}


sub get_farthest_ancestors{
	my ($self, $node, $ffun)=@_;
  #my $trait_count = keys $traits;
	my $n=$node;
  my $traits ={};
  my $trait_nodes={};

	for (my $i=0;$i<100;$i++) { # TODO: 10 -> cfg parameter
		my $nr = $self->{nodes}->[$n];
    my @node_traits = $ffun->($n,$nr) ;
    for my $nt (@node_traits) {
      $trait_nodes->{$nt}=$n;
    }
		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
      die sprintf "%s: node[$n] incosistent: %s", (caller 0)[3], dumper($nr);
			#return ();
		}
		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			last;
		}
	}
  return $trait_nodes;
}




sub get_closest_ancestor {
	my ($self, $node, $ffun)=@_;
	my $n=$node;
	for (my $i=0;$i<100;$i++) { # TODO: 10 -> cfg parameter
		my $nr = $self->{nodes}->[$n];
    if (ref $ffun ne  'CODE' ||  $ffun->($n,$nr) ) {
      return ($n);
    }

		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
      die sprintf "get_farthest_ancestor: node[$n] incosistent: %s", dumper($nr);
			#return ();
		}
		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			last;
		}
	}
  return ();
}

sub get_farthest_ancestor {
	my ($self, $node, $ffun)=@_;
	my $n=$node;
  my ($last, $lastr);
	for (my $i=0;$i<100;$i++) { # TODO: 10 -> cfg parameter
		my $nr = $self->{nodes}->[$n];
    if (ref $ffun ne  'CODE' ||  $ffun->($n,$nr) ) {
      $last= $n; $lastr =$nr;
    }

		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
      die sprintf "get_farthest_ancestor: node[$n] incosistent: %s", dumper($nr);
			#return ();
		}
		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			last;
		}
	}
  return ($last);
}

sub old_get_root {
my ($self, $node)=@_;
	my $n=$node;
	for (my $i=0;$i<10;$i++) {
		my $nr = $self->{nodes}->[$n];
		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
			die "parent $n of node $node is inconsistent";
		}
		if ($nr->[0]{parent}) {
			$n=$nr->[0]{parent}
		}
		else {
			return $n;
		}
	}
}

sub delete_results {
  my ($self,$query_node,$request_id)=@_;
  my $request  = $query_node->get_child($request_id);
  if (!defined $request) {
    die sprintf "request %s of node %s not found", $request_id, $query_node->id
  }
  my $qid = $query_node->id;

  my $results= $self->results;
  for my $url (keys %$results) {
    if (exists $results->{$url}{$qid}{$request_id}) {
      delete $results->{$url}{$qid}{$request_id}
    }
    if (!keys %{$results->{$url}{$qid}}) {
      delete $results->{$url}{$qid};
    }
    if (!keys %{$results->{$url}}) {
      delete $results->{$url};
    }
  }
  delete $request->{data}{results};
  delete $query_node->data->{relevance};
}

sub add_result {
  			#my $res_index = $self->add_result('fiddler', $query_node, $request_id, $request_guid, $order++, $url, $title, $text);
	my ($self, $source, $node, $request_id, $guid, $order, $url, $title, $text)=@_;
	$guid//=$def_guid;

  my $request  = $node->get_child($request_id);
  if (!defined $request) {
    die sprintf "request %s of node %s not found", $request_id, $node->id
  }
  my $data = $request->{data};
	my $result = {id=>$request_id, type=>'result', order=>$order, url=>$url, title=>$title, text=>$text} ;
  my $existing  = $self->results->{$url}{$node->id}{$request_id};
  if (defined $existing) {
    $data->{results}[$existing] = $result;
  }
  else {
    push @{$data->{results}}, $result;
    my $i = @{$data->{results}}-1;
    $self->results->{$url}{$node->id}{$request_id}=$i;
  }
  $self->changed(1);
}

sub inherit {
	my ($self, $parent, $child) =@_;
  if ($self->{nodes}->[$child][0]{parent}!=$parent) { # not checking children, should be enough
    $self->{nodes}->[$child][0]{parent}=$parent;
    push @{$self->{nodes}->[$parent][0]{children}}, $child;
  }
}

sub add_browser_data {
  my ($self, $_args)=@_;
  my $allargs;
  if (ref $_args eq 'HASH') {
    $allargs = [$_args];
  }
  else {
    $allargs = $_args;
  }

  for my $args (@$allargs) {

    #if (($args->{type}!~ m{^(query|query_idea|jump|new_tab|tab_link|nav_typed|nav_link|nav_form_submit)})) {
    if ($args->{type} !~ /^(query|query_idea|new_tab|(nav|tab)_(typed|form_submit|jump|link))$/) {
      die "add_browser_data: wrong type: $args->{type}\n",
    }
		my $edge_type=$args->{type};

    #my $node_type;
    my $request_type = $args->{request_type}//='request';
    my $node_type =$args->{node_type};

    my $url = $args ->{url};
    my $urlkey = $args->{key}//"url:$url";

    my $query = $args ->{query};
    my $querykey= $args->{querykey};
		if (!defined $querykey && $query ne '') {
			$querykey = "google:$query";
		}
    if (defined $querykey) {
      $node_type//='query';
    }

    #my $type =  $args ->{type};
    my ($peer_type );
    my $peer = $args->{parent} // $args->{peer}; #fix: parent to peer
    my $pkey = $args->{peerkey};

    #$peer_type = $args->{peertype} // ($type eq 'query_idea' ? 'url' : $type);
		$peer_type= $args->{peertype};
    my ($phost, $pquery);
    if (!defined $pkey) {
      if ( ($phost, $pquery) = $peer =~ m{^\w+://(www\.google\.(?:com|ru))/search.*?[?&]q=([^&]*)} ) {
          #$pquery=~s{%([\da-f]{2})}{chr hex $1}egi;
          #$pquery=decode('utf8',$pquery);
          $pquery=url_unescape($pquery);
          $pkey ="google:$pquery";
          $peer_type//='query';
      }
      else {
        $pkey //="url:$peer"
      }
    }

    my $peer_node ;
		my $from;
		$from = $self->g->get_node_by_key($pkey)
			if ($pkey ne '');

		#if ($from) {
		#	$peer_type//=$from->data->{node_type};
		#}
    if ($request_type eq 'request') {
			if (!$from ) {
				if ($peer_type ne '') {
          my $meta={ data=>{node_type=>$peer_type, source=>'browser', url=>$peer}
																					 , partition=>$self->current, id=>$self->next_id};
          if ($peer_type eq 'query') {
            $meta->{data}{query}=$pquery if defined $pquery;
            $meta->{key}=$pkey;
          }
					$from = graph_object::node->new($meta);
					if (!defined $self->g->add_node( $from )) {
						die "failed to add the node\n";
					}
				}
			}
			else {
				if ($peer_type ne '' && $peer_type ne $from ->data->{node_type}) {
					die sprintf "peer type mismatch :%s <> %s", $peer_type, $from->data->{node_type};
				}
				$peer_type= $from->data->{node_type};
			}
      #$peer_node = get_node($self,$pkey) || get_or_add_node($self,$pkey, {node_type=>'url', source=>'browser', url=>$peer});
    }
		if (!$from && !$args->{peer_optional}) {
      die sprintf "add_browser_data: cannot find from node: %s", dumper($args);
    }

    if ($request_type eq 'request') {
			#my $tkey;
      if (!defined $querykey) {
        my $host;
        if ( ($host, $query) = $url =~ m{^\w+://(www\.google\.(?:com|ru))/search.*?[?&]q=([^&]*)} ) {
          #$query=~s{%([\da-f]{2})}{chr hex $1}egi;
          $query=url_unescape($query);
          $querykey="google:$query";
          $node_type//='query';
        }
      }

      my $key;
      if (defined $querykey) {
        if ($node_type ne 'query' ) {
          die sprintf "node_type should be 'query': %s", dumper($args);
        }
        $key = $querykey;
      }
      else {
        if($node_type eq 'query' ) {
          die sprintf "cannot derive query key for: %s", dumper($args);
        }
        $node_type//='url';
        $key = $urlkey;
      }
			#if ($edge_type eq 'query' || $edge_type eq 'query_idea') {
			#	if (!defined $querykey) {
			#		die sprintf "cannot derive query key for: %s", dumper($args);
			#	}
			#	#$tkey=$querykey;
			#}
			if (!defined $key) {
        die sprintf "key for new node not defined :%s", dumper($args);
			}
			my $to= $self->g->get_node_by_key($key);
      if (!defined $to ){
				if ($node_type eq 'query' ) {
          my $meta = { id=>$self->next_id, data=>{node_type=>'query', source=>'browser'
																, query=>$query, url=>$url}, partition=>$self->current};
					$to = graph_object::node->new($meta);
				}
				else {
          my $meta = { id=>$self->next_id, key=>$key, data=>{node_type=>'url'
										, source=>'browser', url=>$url}, partition=>$self->current};
	        $to = graph_object::node->new($meta);
				}
        if (!defined $self->g->add_node( $to )) {
          die "failed to add the node\n";
        }
      }
			my $ts=time();
			my $children = $to->find_children({ start_ts=>$ts-$self->{cfg}{request_ts_range}, end_ts=>$ts+$self->{cfg}{request_ts_range}});
      my $child_id;
      if (!$children) {
        $child_id = $to->add_child({source=>'browser', ts=>$ts},$to->id, $self->next_id);  # FIX: correct id after read_db
        if (!defined $child_id) {
          die sprintf "!defined \$child_id for %s, %s ", $to->key, $to->id;
        }
      }
      else {
        $child_id = shift @$children;
      }

      #my $req_node=add_node($self,{source=>'browser', node_type=>$node_type, ts=>time()});

      #my $url_node= get_node($self,$key) || get_or_add_node($self,$key, {node_type=>'url', url=>$url});
      #inherit($self,$url_node, $req_node);

			if ($from) {
        my $meta = { from=>$from->id, to=>$to->id, child_id=>$child_id,
                    data=>{ edge_type=>$edge_type//'referer'}, partition=>$self->current};
        my $found = $self->g->find_edges( $meta );
        if (!$found || !$found->[0]) {
    #my ($self,$data,$from,$to,$id, $from_child, $to_child)=@_;
          $meta->{id}=$self->next_id;

          my $edge=graph_object::edge->new($meta);
          if (!defined $self->g->add_edge($edge)) {
            die "cannot add new edge";
          }
        }
			}
    }
    #else {
    #  my $new_node=add_node($self,{source=>'browser', node_type=>$node_type, ts=>time()});
    #
    #  add_peers($self, $peer_type,[$peer_node] , [$new_node]) if $peer_node && $new_node;
    #}
  }
  $self->changed(1);
  return 1;
}




sub get_descendants_depth_last {
  my ($self,$nodes)=@_;
  my ($nodes_detach)={};
  my $ffun = sub {
    my ($node, $nr, $level)=@_;
    $nodes_detach->{$node}++;
  };
  for my $node (keys %$nodes) {
    descendants_depth_last($self, $node, $ffun);
  }
  return $nodes_detach;
}


sub max_ts {
  my ($self,$parts)=@_;
  return () if ref $parts ne 'ARRAY';
  my $mx;
  for my $part(@$parts) {
    $mx //= my $ts = $self->partition_ts($part);
    $mx = $ts if $mx < $ts;
  }
  return $mx;
}

sub merge_data {
  my ($self, $snode, $anode, $smx, $amx)=@_;
  my $data={};
  my $method= $self->{cfg}{merge_method};
  my ($sdata,$adata)=($snode->data, $anode->data);
  my $ok;
  if ($sdata->{node_type} ne $adata->{node_type}) {
    die sprintf "cannot merge nodes %s, %s: node types are different\n", dumper($snode),dumper($anode);
  }

  if ($sdata->{node_type} eq 'topic' && $sdata->{topic} ne $adata->{topic}) {
    die sprintf "cannot merge nodes %s, %s: topics are different\n", dumper($snode),dumper($anode);
  }
  if ($sdata->{node_type} eq 'query' &&  $sdata->{query} ne $adata->{query}) {
    die sprintf "cannot merge nodes %s, %s: queries are different\n", dumper($snode),dumper($anode);
  }
  if ($sdata->{node_type} eq 'url' && $sdata->{url} ne $adata->{url}) {
    die sprintf "cannot merge nodes %s, %s: urls are different\n", dumper($snode),dumper($anode);
  }

  if ($method==1) {
    my $keys = {} ;
    for my $key((keys %$adata), (keys %$sdata)) {
      next if $keys->{$key}++;
      if ($key =~/^(node_type|url|query|topic)$/ ){
        $data->{$key} = ($sdata->{$key}//$adata->{$key});
      }
      elsif ($key eq 'rating') {
        $data->{rating} = List::Util::max($sdata->{rating}//$adata->{rating}, $adata->{rating}//$sdata->{rating} );
      }
      elsif ($key eq 'ts') {
        $data->{rating} = List::Util::max($sdata->{ts}//$adata->{ts}, $adata->{ts}//$sdata->{ts} );
      }
      elsif ($key eq 'comment') {
        $data->{comment} = join "\n##\n", grep { $_ ne '' } $sdata->{comment}, $adata->{comment};
      }
      elsif ($key eq 'source') {
        $data->{source} = 'browser' if $sdata->{source} eq 'browser' || $adata->{source} eq 'browser' ;
        $data->{source} //=($sdata->{source}//$adata->{source});
      }
      elsif ($key eq 'relevance') {
        $data->{relevance} = $smx < $amx ? $adata->{relevance} : $sdata->{relevance};
        $data->{relevance} //= ($adata->{relevance} // $sdata->{relevance});
      }
      else {
        die sprintf "cannot merge nodes %s, %s: don't know how to merge %s\n", dumper($snode),dumper($anode), $key;
      }
    }
  }
  elsif ($method==2) {
    for my $key(keys %$adata) {
      $sdata->{$key} = ($sdata->{$key} //$adata->{key}) ;
    }
  }
  elsif ($method==3) {
    common::hash_extend($sdata,$adata);
  }
  else {
    die sprintf "cannot merge nodes %s, %s: merge method unknown\n", dumper($snode),dumper($anode);
  }
  return $data;
}

sub merge_children {
  my ($self, $snode, $anode)=@_;
  my $adesc = $anode->{descendants};
  if (!defined $adesc) {
    return ()
  }
  my $ndesc = ($snode->{descendants}//{});

  my $proc = sub {
    my ($id ,$node, $parent,$root, $level)=@_;
    if ($node == $root ) {
      return 0 ;
    }
    my ($ch);
    $ch = $snode->get_child($id);
    if (!defined $ch) {
      (undef, $ch)  = $snode->add_child($node->{data},$snode->id, $id);  # FIX: correct id after read_db
    }
    else {
      my $ares = $node->{data}->{results};
      my $sres = $ch->{data}->{results};
      if (defined $ares && defined $sres) {
        if (keys %$ares!=keys %$sres) {
          die sprintf "cannot merge nodes %s, %s: key counts: %d, %d\n", dumper($snode),dumper($anode), 0+keys %$sres, 0+keys %$ares;
        }
      }
      $ch->{data}->{results}//=$ares;
    }
    0;
  };

  $anode->descend_dl(undef, $anode, $proc, 0); # FIX: initial parent can be undef//=$anode
}

sub merge_node {
  my ($self,$att, $node, $att_node, $smx,$amx)=@_;

  if ($smx< $amx) {
    $node->data($self->merge_data($att_node,$node,$amx,$smx));
  }
  else {
    $node->data($self->merge_data($node,$att_node,$smx,$amx));
  }
  $self->merge_children($node,$att_node);
  $self->g->assign_elt_partition($node,$att->current,0);
}


sub check_derived {
  my ($self, $outer, $inner)=@_;
  my $o_desc = $outer->{descendants};
  my $i_desc = $inner->{descendants};
  if ($i_desc) {
    if (!$o_desc) {
      die sprintf "node %s is not derived from node %s\n", $inner->id, $outer->id
    }
    return 0 if (!$o_desc);
    for my $desc(keys %$i_desc) {
      if (!defined $o_desc->{$desc}) {
        die sprintf "node %s is not derived from node %s\n", $inner->id, $outer->id
      }

      my $cmp = Compare($i_desc->{$desc}, $o_desc->{$desc});
      if (!$cmp ) {
        die sprintf "childs differ: inner %s->%s, outer %s->%s\n"
              , $inner->id, dumper($i_desc->{$desc}), $outer->id, dumper($o_desc->{$desc});
      }
    }
  }
}

sub latest_partition {
  my ($self,$partitions)=@_;
  #return () if ref $partitions ne 'ARRAY';
  my ($latest_ts,$latest);
  for my $p (@$partitions) {
    my $ts=  $self->partition_ts($p);
    if (!defined $latest || $ts gt $latest_ts) {
      $latest=$p;
      $latest_ts= $ts;
    }
  }
  return $latest;
}

sub merge_graphs_show_stats {
  my ($self,$att)=@_;
  my $gstats = $self->merge_graphs($att);
  $self->changed(1);
  for (sort keys %$gstats) {
    printf STDERR "merge_graphs: %s=%s\n", $_, $gstats->{$_};
  }
}




sub merge_graphs {
  my ($self,$att,$args)=@_;
  $args//={};
  my $nodes=$self->g->nodes;
  my $edges=$self->g->edges;

  my $stats={qw(nodes_added 0 nodes_merged 0 edges_added 0 edges_merged 0)};
  my $merged_nodes = {};
  my $merge_map = {};

  for my $nid (sort keys %{$att->g->nodes}) {
    my $an = $att->g->get_node_by_id($nid);
    my $sn = my $sn_by_id= $self->g->get_node_by_id($nid);
    if (!defined $sn) {
      $sn = $self->g->get_node_by_key($an->key);
    }
    if (defined $sn ) {
      my $snp=[grep {$self->{_selection}{$_}{loaded} ne ''}  graph_object::node::partitions($sn)];

      my $snc = $self->latest_partition($snp);
      my $anc = $att->current;
      if (!defined $self->{storage}{$anc}) {
        die sprintf "attachment partition '%s' does not exist", $anc;
      }
      my $smx= $self->partition_ts($snc);
      my $amx= $att->partition_ts($anc);
      if (!$sn_by_id && $self->{cfg}{merge_method}) {
      #if ($self->{cfg}{merge_method}) {
        if (defined $merge_map->{$an->id} ) {
          die sprintf "node %s was already merged with %s\n", $an->id, $merge_map->{$an->id};
        }
        $self->merge_node($att,$sn,$an,$smx,$amx);
        $merge_map->{$an->id}=$sn->id;
      }
      elsif ($smx  <  $amx ) {
        $self->check_derived($an,$sn);
        $sn->data($an->data);
        #$sn->edges($self->g->derive_edges());
      }
      $merged_nodes->{$sn->id} = $sn;
      $stats->{nodes_merged}++;
    }
    else {
      my $meta = $an;
			(undef,$sn)= $self->g->add_node($an);
      $stats->{nodes_added}++;

      #$nodes->[$an]=$att->{nodes}[$an];
    }
		#$self->g->assign_elt_partition($sn,$self->{current});
    #descendants_depth_last($att,$root,$merge_root);

  }

  my $fix_edges={};

  for my $eid (sort keys %{$att->g->edges}) {
    my $ae = $att->g->get_edge_by_id($eid);
    my $se = $self->g->get_edge_by_id($eid);
    if (defined $se) {
      my $seq = Clone::clone($se);
      my $aeq = Clone::clone($ae);
      delete $seq->{partition};
      delete $aeq->{partition};
      my $cmp = Compare($seq, $aeq);
      if (!$cmp) {
        die sprintf "edge %s differs in partitions: previous: %s, being loaded:%s\n",$eid, $seq->partition, $att->current ;
        #die sprintf "edge %s already in target graph",$eid;
      }
      if (defined $merge_map->{$ae->from} ) {
        die sprintf "duplicate edge %s(%s) has 'from' node in merge_map:%s->%s"
        , $ae->id, $att->current, $ae->from, $merge_map->{$ae->from}
      }
      if (defined $merge_map->{$ae->to} ) {
        die sprintf "duplicate edge %s(%s) has 'to' node in merge_map:%s->%s"
        , $ae->id, $att->current, $ae->to, $merge_map->{$ae->to}
      }
      $self->g->assign_elt_partition($se,$att->current,0);
      my @partitions = $se->partitions;
      if (@partitions!=1) {
        my $partition = (pop @partitions) // $att->current;
        $self->g->assign_elt_partition($se,$partition,1);
      }
    }
    else {
      if ($merge_map->{$ae->from}) {
        $ae->from($merge_map->{$ae->from});
        $fix_edges->{$ae->id}++;
      }
      if ($merge_map->{$ae->to}) {
        $ae->to($merge_map->{$ae->to});
        $fix_edges->{$ae->id}++;
      }

      if (!$self->g->add_edge($ae)) {
        die sprintf "cannot add edge %s", $eid
      }
      #$self->g->assign_elt_partition($se,$self->{current});
      $stats->{edges_added}++;
    }
  }

  my $fix_nodes={};
  for my $eid (keys %$fix_edges) {
    my $e = $self->g->get_edge_by_id($eid);
    $fix_nodes->{$e->from}++;
    $fix_nodes->{$e->to}++;
  }
  for my $nid (keys %$merged_nodes) {
    $fix_nodes->{$nid}++;
  }

  unless ($args->{partial}) { # we are to write merged nodes instead of original ones back to the attached partition
    for my $nid (sort keys %$merge_map) {
      $att->g->delete_node($nid);
    }
  }
  $self->g->derive_edges_for_nodes($fix_nodes);
  #while (my ($nid,$sn) = each %$merged_nodes) {
  #  $sn->edges($self->g->derive_edges($sn));
  #}

  return $stats;
}




######## storage

#pack age query_graph_object::storage;
#use common qw(:DEFAULT :TS);
#use strict;

#our $query_graph_db;

sub init_db {
  my ($self, $storage)=@_;
  my $t;
  #for ('$self','$self->{cfg}', '$self->{cfg}{storage}') {
  #  warn sprintf "is_shared(%s)=%d\n", $_, eval {qq{is_shared($_)}};
  #}

  #warn sprintf "is_shared(\$self)=%d,is_shared(\$self->{cfg})=%d\n", is_shared($self), is_shared($self->{cfg});

  $self->{cfg}{storage}=  $storage if $storage ne '';
  $self->{storage}={};
  $self->{selection}={};
  $self->{_selection}={};
	$self->{request_files}={};
	$self->{results}={};  # { url, { [ guid, text ], rating }  }
  #$self->{nodes}={};
  #$self->{edges}={};
	$self->{_nar}=[];
	#$self->{nodemap}={};
	$self->{_ear}=[];

  #if ($self->{cfg}{shared}) {
  #  $self=shared_clone($self);
  #  warn sprintf "is_shared(\$self)=%d,is_shared(\$self->{cfg})=%d\n", is_shared($self), is_shared($self->{cfg});
  #
  #}

  $default_metadata = {
    selection=>{},
    #selection=>{core=>{mode=>'rw'}},
    #selection=>{core=>{mode=>'rw'}, referer160101=>{mode=>'rw'}},
    request_files=>{},
    storage=>{ core=>$self->get_partition_file('core'),
               referer160101=>$self->get_partition_file('referer160101'),
             },

    request_files=>{},
    next_id=>0,
    dict_ru=>'dict_ru',
    dict_en=>'dict_en',
    #current=>undef,
  };

  common::hash_extend($self,$default_metadata);
  print STDERR "using json storage: '$self->{cfg}{storage}'\n";
  $self->read_db();
  kmg_stats::init_lemmatizer($self->get_partition_filepath($self->{dict_ru}),$self->get_partition_filepath($self->{dict_en}));
  #if (-e $self->{cfg}{storage} ) {
  #  $self->read_db();
  #}
}

sub read_json {
  use JSON::XS;
  my ($file,$json,$encoding)=@_;
  if (!defined $json) {
    $json=JSON::XS->new->canonical->convert_blessed;
  }
  $encoding//='utf8';
  my $jsondata;
  open(my $fh, "<".enc($encoding), $file) or die "cannot open $file:$!";
  read $fh, $jsondata, -s $fh;
  close $fh;
  return $json->decode($jsondata);
}

sub load_json {
  my ($self, $jsd, $nodeset, $edgeset)=@_;
  my $nodes = $jsd->{nodes};
  my $edges = $jsd->{edges};

  if (ref $nodes eq 'HASH') {
    if ($nodeset) {
      $nodes = common::sub_hash($nodes, $nodeset);
    }
    $self->g->nodes($nodes);
  }
  else {
    if ($nodeset) {
      die sprintf "nodeset not supported for an array of nodes";
    }
    $self->{_nar}=$jsd->{nodes};
  }
  if (ref $edges ) {
    if ($edgeset) {
      $edges = common::sub_hash($edges,$edgeset);
    }
    $self->g->edges($edges);
  }
  if ($jsd ->{links}) {
    if ($edgeset) {
      die sprintf "edgeset not supported for an array of edges";
    }
    $self->{_ear}=$jsd->{links};
  }
  if (!$self->{request_files} || !keys %{$self->{request_files}}) {
    if (defined $jsd->{request_files} ) {
      $self->{request_files}=$jsd->{request_files};
    }
  }
  #$self->changed(1);
}

sub save_json {
    my ($obj,$storage)=@_;
    my $tempfile1 = common::newfile($storage);
    open(my $fh, ">".enc($db_encoding), $tempfile1) or die "cannot write to tempfile '$tempfile1': $!";
    print {$fh} $obj;
    close $fh;
    die "storage is a directory: '$storage'" if (-d $storage);
    File::Copy::move($tempfile1,$storage) or die "cannot move '$tempfile1' to '$storage' :$!";
  }


sub _get_storage_filename {
  my ($self,$type)=@_;
  if ($type eq 'core') {
    return 'core_graph.json';
  }
  elsif ($type eq 'junk') {
    return 'junk_graph.json';
  }
  elsif ($type eq 'registry') {
    return 'registry.json';
  }
  else {
    return "$type.json";
  }
}

sub get_partition_file {
  my ($self, $file)=@_;
  #my $filename= $self->get_partition_filename($type);
  if ($file=~m{/}) {
    return $file;
  }
  if ($file!~m{\.json$}) {
    return $file.='_graph.json';
  }
  return $file;
}

sub get_partition_filepath {
  my ($self, $file, $storage)=@_;
  #my $filename= $self->get_storage_filename($type);
  my $fullname=$self->get_partition_file($file);
  if ($fullname=~m{/}) {
    return $fullname;
  }
  return +($self->{cfg}{storage}//'.')."/$fullname";
}

sub selection_str {
  my ($self,$selection,$current)=@_;
  if (!defined $selection) {
    $selection = $self->{selection}//{};
    $current = $self->current
  }

  my $sel = "$current: ". join ' ', map {
      my $mode=$selection->{$_};
      if($_ eq $current) { $mode=~s{^C?}{C}; }
      qq{$_=$selection->{$_}{mode}}
    }  sort keys %{$selection};
};

sub clear {
  my ($self)=@_;
  $self->{_selection}={};
  $self->{results}={};
  $self->{g} = graph_object->new(common::sub_hash( $default_cfg, graph_object::default_cfg() )  );
}

sub change_selection {
  my ($self,$cfg_selection,$traits)=@_;
  $traits//={};
  $traits->{write}//=1;
  $traits->{clear}//=1;
  #my $cfg_selection = $self->{cfg}{select};
  my ($new_selection,$current) = $self->new_selection($cfg_selection,$traits->{current});
  printf STDERR "old selection: %s\nnew selection: %s\n", $self->selection_str(),$self->selection_str($new_selection,$current);
  my $selection = $self->{selection};
  my $changed;
  my $clear;
  eval  {
    for my $sel (keys %{$self->{_selection}}) {
      if ($self->{_selection}{$sel}{changed}) {
        if (!$new_selection->{$sel}) {
          $changed++;
        }
      }
      if ($self->{_selection}{$sel}{loaded} eq 'complete' && !$new_selection->{$sel}) {
        $clear++;
      }
    }
  };
  if ($@) {
    die sprintf "exception: %s\n, %s", $@, Dumper({
                                                   _sel=>$self->{_selection},
                                                   refsel=>ref $self->{_selection},
                                                   refself=>ref $self,
                                                   })
  }
  if ($changed && $traits->{write}) {
    $self->write_db()
  }
  if ($clear && $traits->{clear}) {
    $self->clear();
  }
  $self->{selection}=$new_selection;
  $self->{current}=$current;
  $self->read_storage();
}

sub new_selection {
  my ($self, $cfg_selection,$new_current)=@_;
  #my $cfg_selection = $self->{cfg}{select};
  #printf STDERR "current selection: (%s)\n", join ' ', $cfg_selection ? @$cfg_selection : () ;
  my $new_selection=Clone::clone($self->{selection});
  #my $current =$self->current;
  my $suggested_current;
  if (ref $cfg_selection eq 'ARRAY') {
    if (grep { /^none$/ } @$cfg_selection) {
      $new_selection={};
      #$self->current(undef,undef);
    }
    if (grep { /^default$/ } @$cfg_selection) {
      $new_selection=$default_metadata->{selection};
      #$self->current(undef,undef);
    }
    for my $selection(@$cfg_selection) {
      next if $selection=~/^(none|default)$/;
			if ($selection=~/^new$/) {
        my $_current = common::ts_compact();
        $suggested_current//=$_current;
        #if (!$new_selection->{$_current}) {
        #  $suggested_current=$_current;
        #}
				#$self->current(undef,$partition);
				$new_selection->{$_current}={mode=>'rw'};
			}
      if ($selection=~/^today$/) {
        my $_current = common::tsd_compact();
        $suggested_current//=$_current;
        #if (!$new_selection->{$current}) {
        #  $current=$_current;
        #}
				#$self->current(undef,$partition);
				$new_selection->{$_current}={mode=>'rw'};
			}
      elsif ($selection=~/^day:(\d+)(?:-(\d+))\s*(?:=\s*(.*?)\s*)?$/) {
        my ($day1,$day2,$mode)=($1,$2,$3);
        $mode//='rw';
        $day2//=$day1;
        #$day2 = $day1 if $day2<$day1;
        for (my $day=$day1;$day<=$day2;$day++) {
          my $partition = common::tsd_compact(time()-$day*86400);
          next if !defined $self->{storage}{$partition};
          #next if $new_selection->{$partition};
        #$suggested_current//=$_current;
        #if (!$new_selection->{$current}) {
        #  $current=$_current;
        #}
				#$self->current(undef,$partition);
          $new_selection->{$partition}{mode}=$mode;
          if ($mode=~s{C}{}i) {
            $suggested_current= $partition;
          }
        }
			}
      elsif ($selection=~/^\s*\{(.*)\}\s*(?:=\s*([a-z]+)\s*)?$/i) {
        my ($filter,$mode)=($1,$2);
        $mode//='rw';
        for my $partition (sort keys %{$self->{storage}}) {
          next unless ($partition =~ /$filter/);
          $new_selection->{$partition}{mode}=$mode;
          if ($mode=~s{C}{}i) {
            $suggested_current= $partition;
          }
        }
      }
      elsif ($selection=~/^\s*([^=]+?)\s*(?:=\s*([a-z]+)\s*)?$/i) {
        my ($partition, $mode)=($1,$2);
        $mode//='rw';
        $new_selection->{$partition}{mode}=$mode;
        if ($mode=~s{C}{}i) {
          $suggested_current= $partition;
        }
      }
        #if ($mode=~/v/) {
        #  $current = $partition;
        #  $new_selection->{$partition}{}
        #  $self->current(undef,$partition);
        #}
        #elsif ($mode=~/f/) {
        #  $self->current($partition);
        #}
        #elsif (!defined $self->current) { # if fixed or volatile is not set, set fixed
        #  $self->current(undef,$1);
        #}
        #$new_selection->{$partition}={mode=>$mode};
    }
  }

  $suggested_current=$new_current if defined $new_current;
  if (!defined $suggested_current) {
    if ($new_selection->{$self->current}) {
      $suggested_current = $self->current;
    }
  }
  if (!defined $suggested_current) {
		$suggested_current = common::tsd_compact();
  }
  if (!defined $new_selection->{$suggested_current}) {
    $new_selection->{$suggested_current}={mode=>'rw'};
	}
  return ($new_selection, $suggested_current);
}

sub partition_buckets{
	my ($self)=@_;
  my ($linked)={};
	#die "ref \$storages != ARRAY" if ref $storages ne 'ARRAY';
	my ($buckets)={};
	my $nodes= $self->g->nodes;
	my $edges= $self->g->edges;
	for my $nid(keys %$nodes) {
		my $node= $self->g->get_node_by_id($nid);
		my @partitions = $node->partitions;
    #my @pkeys;
		for my $partition((@partitions)) {
			$buckets->{$partition}{nodes}{$nid}=$node;
		}
    for (my $i=0;$i<@partitions;$i++ ) {
      for (my $j=$i+1;$j<@partitions;$j++) {
        next if $partitions[$i] eq $partitions[$j];
        $linked->{$partitions[$i] }{$partitions[$j]}++;
        $linked->{$partitions[$j] }{$partitions[$i]}++;
      }
    }
	}
	for my $eid(keys %$edges) {
		my $edge= $self->g->get_edge_by_id($eid);
    my @partitions = $edge->partitions;
    my $partition = @partitions ? (pop @partitions) : $self->current;
    $buckets->{$partition}{edges}{$eid}=$edge;
#		for my $partition((@partitions)) {
#
#		}
#    for (my $i=0;$i<@partitions;$i++ ) {
#      for (my $j=$i+1;$j<@partitions;$j++) {
#        next if $partitions[$i] eq $partitions[$j];
#        $linked->{$partitions[$i] }{$partitions[$j]}++;
#        $linked->{$partitions[$j] }{$partitions[$i]}++;
#      }
#    }
		#
		#for my $partition(split m{/}, $partitions) {
		#	$buckets->{$partition}{edges}{$eid}=$edge;
		#}
	}
  for my $partition(keys %$linked) {
    $buckets->{$partition}{partitions}=$linked->{$partition};
  }

	return $buckets;
}


sub read_db {
  my ($self, $storage)=@_;
  $self->read_metadata($storage);
  my $selection = $self->{cfg}{select};
  $self->change_selection($selection);
}

sub read_metadata {
  my ($self, $storage)=@_;
  $storage  = ($self->{cfg}{storage}//=$storage); # in case read_db called once per initialization
  if (!-d $storage ) {
    die "storage '$storage' is not a directory";
  }

  my $metadata_partition = $self->get_partition_filepath("metadata");
  if (-e $metadata_partition) {
    my $metadata=read_json ($metadata_partition, $json, $db_encoding);
    common::hash_extend($self,$metadata);
  }
  for my $st ( keys %{$self->{storage}}) {
    if (ref $self->{storage}{$st} ne 'HASH') {
      if ($self->{storage}{$st} ne '') {
        $self->{storage}{$st}={file=>$self->{storage}{$st}};
      }
    }
  }
}

sub read_storage {
  my ($self, $storage)=@_;
  use Benchmark;
  my ($t0,$t1);
  $t0 = Benchmark->new;

  my $current=$self->current;

  #if ($self->{selection}{$current}{mode}=~/r/) {
  if ($self->{_selection}{$current}{loaded} ne 'complete' ) {
    my $t0 =Benchmark->new;
    $self->read_partition($current,1);
    my $t1 = Benchmark->new;
    printf STDERR "main load: %s, times: %s\n", $current, timestr(timediff($t1,$t0));
    $self->{_selection}{$current}{loaded}='read';
    $self->{_selection}{$current}{changed}=0;
  }
  #$self->g->assign_partition($current,0);
  #}
  if ($self->{selection}) {
    for my $partition(sort keys %{$self->{selection}}) {
      next if $partition eq $current;
      if ($self->{_selection}{$partition}{loaded} eq 'complete') {
        next;
      }
      my $att=bless({g=>bless ({nodes=>{},edges=>{}},'graph_object'), cfg=>$self->{cfg}}, __PACKAGE__);
      my $t0 =Benchmark->new;
      $att->read_partition($partition,1);
      my $t1 = Benchmark->new;
      printf STDERR "attachment load: %s, times: %s\n", $partition, timestr(timediff($t1,$t0));

      eval {
        $self->merge_graphs($att);
      };
      if ($@) {
        die $@;
      }
      $self->{_selection}{$partition}{loaded}='read';
      $self->{_selection}{$partition}{changed}=0;

      $self->derive_data();
      if ($self->{cfg}{debug}) {
        $self->consistency_check();
      }
    }
  }

  my ($buckets)=  $self->partition_buckets();
  for my $partition(sort keys %{$buckets}) {
    my $node_set = {};
    my $edge_set ={};
    next if $self->{selection}{$partition}; # || $partition eq $current;
    my $att=bless({g=>bless ({nodes=>{},edges=>{}},'graph_object'), cfg=>$self->{cfg}}, __PACKAGE__);
    for my $linked (keys %{$buckets->{$partition}{partitions}}) {
      next if !$self->{selection}{$linked}; # searching for non-selected($partition) that intersect with selected($linked)
      next if $self->{_selection}{$linked}{loaded} eq  'complete';
      next if ($self->partition_ts($partition) < $self->partition_ts($linked)); # FIX: < must be <=
      # invariant: partitionA.ts > partitionB.ts => any id: A{id}.ts >= B(id).ts
      # already loaded nodes are the most recent
      # the logic: intersection of two partitions minus what was already loaded
      my $node_and = common::sub_hash($buckets->{$partition}{nodes}, $buckets->{$linked}{nodes});
      my $edge_and = common::sub_hash($buckets->{$partition}{edges}, $buckets->{$linked}{edges});
      common::hash_extend($node_set, $node_and);
      common::hash_extend($edge_set, $edge_and);
    }
    if (!!keys %$node_set || !!keys %$edge_set ) {
      my $t0 =Benchmark->new;
      $att->read_partition($partition,0,$node_set, $edge_set);
      my $t1 = Benchmark->new;
      printf STDERR "partial load: %s, times: %s\n", $partition, timestr(timediff($t1,$t0));
      #$att->current($partition);
      $self->merge_graphs($att,{partial=>1});
      $self->{_selection}{$partition}{loaded}='partial';
      $self->{_selection}{$partition}{changed}=0;
      $self->derive_data();
      if ($self->{cfg}{debug}) {
        $self->consistency_check();
      }
    }
  }

  while(my($key,$ref)= each %{$self->{_selection}}) {
    if ($ref->{loaded} eq 'read') {
      $ref->{loaded} = 'complete';
    }
  }

  $self->derive_data();
  $self->consistency_check();

  $self->changed(0);
}

sub read_partition  {
  my ($self,$partition,$post_actions,$nodeset, $edgeset)=@_;
  my $filepath = $self->get_partition_filepath($partition);

  if (-e $filepath) {
    my $file_ts= (stat[$filepath])[9];
    #my $t0 =Benchmark->new;
    my $jsd = read_json($filepath, $json, $db_encoding);
    #my $t1 = Benchmark->new;
    #printf STDERR "decoded %s: time %s\n", $filepath, timestr(timediff($t1,$t0));
    $self->load_json($jsd,$nodeset,$edgeset);
    my $ts = $jsd->{timestamp}//$self->partition_ts($partition);
    $self->{storage}{$partition}{timestamp}//=$ts;
    if ($self->{storage}{$partition}{timestamp} ne $ts) {
      die sprintf  "timestamp mismatch for %s: [registry]%s <>[file]%s\n"
      , $partition, $filepath,$self->{storage}{$partition}{timestamp}, $ts;
    }
    $self->current($partition); # FIX: $self->origin($partition);
    if ($nodeset || $edgeset) {
      #$self->{selection}{$partition}{partial}=1;
      $self->rebless();
      $self->g->assign_partition($partition,0);
    }

    #$self->g->assign_storage($partition,1);
    if ($post_actions) {
      $self->derive_data();
      $self->g->assign_partition($partition,0);
      $self->consistency_check();
    }
  }
}


sub write_partition {
  my($self, $gdetachment, $partition)=@_;
  #my $nodes=$self->{nodes};
  #my $edges=$self->{links};
  my $filepath = $self->get_partition_filepath($partition);
  if (-d $filepath ) {
    die "partition path '$filepath' is a directory";
  }

  my ($detachment) = map { bless { g=>$_ }, __PACKAGE__ }  ($gdetachment);
  $detachment->consistency_check();
  #$detachment->derive_data();
  my $ts = time();
  if ($self->{storage}{$partition}) {
    $self->{storage}{$partition}{timestamp}=$ts;
  }
  my ($json_detachment) =  $json ->pretty->encode ({
      nodes=>$gdetachment->nodes,
      edges=>$gdetachment->edges,
      timestamp => $ts,
      });

  save_json($json_detachment, $filepath);
  return 1;
}


sub write_db {
  my($self, $storage)=@_;
  #$storage//=$self->{cfg}{storage}; # in case write_db saving to different locations
  $storage  = ($self->{cfg}{storage}//=$storage); # in case write_db saving to one location per initialization
  if (!-d $storage ) {
    die "storage '$storage' is not a directory";
  }

  $self->g->consistency_check();

	#my $selection = $self->{selection};

	my $buckets =$self->partition_buckets();
	for my $partition (sort keys %{$self->{selection}}) {
    #next if $self->{_selection}{$partition}{partial} ne '';
		next if $self->{_selection}{$partition}{loaded} eq 'partial';
		if ($self->{selection}{$partition}{mode}=~/w/) {
      my $changed = $self->{_selection}{$partition}{changed}||$self->{changed};
      if (!defined $self->{storage}{$partition}) {
        $changed=1;
        $self->{storage}{$partition}={file => $self->get_partition_file($partition)};
      }
			if ($changed) {
				my $ndetach = $buckets->{$partition}{nodes}||{};
				my $edetach = $buckets->{$partition}{edges}||{};
				my $g= $self->g->db_subset($ndetach,$edetach);
        delete $self->{selection}{$partition}{changed};
			  $self->write_partition($g, $partition);
        $self->{_selection}{$partition}{changed}=0;
			}
		}
  }
  $self->write_metadata();
  $self->changed(0);
}

sub get_metadata {
  my ($self)=@_;
  common::sub_hash($self, $default_metadata);
}

sub write_metadata {
  my($self, $storage)=@_;
  $storage  = ($self->{cfg}{storage}//=$storage); # in case write_db saving to one location per initialization
  if (!-d $storage ) {
    die "storage '$storage' is not a directory";
  }
  my $metadata = $self->get_metadata();
  my $metadata_partition = $self->get_partition_filepath("metadata");
  save_json($json->pretty->encode($metadata),$metadata_partition);
  1;
}

sub partition_ts {
  my ($self,$partition)=@_;
  my $ts = $self->{storage}{$partition} && $self->{storage}{$partition}{timestamp};
  return $ts if defined $ts;
  my $filepath = $self->get_partition_filepath($partition);
  if (!-e $filepath ) {
    return time();
  }
  return +(stat($filepath))[9];
}



#--------- misc

sub edge_count {
  shift->g->edge_count;
}

sub node_count {
  shift->g->node_count;
}



sub descendants_depth_last {
  my ($self, $nid, $ffun, $level, $pid,@args)=@_;
  #my $n = old_get_node($self,$nid);
  my $nr = $self->{nodes}[$nid];
  return 0 if (ref $nr ne 'ARRAY');
  return 1 if $ffun->($nid, $nr, $level, $pid,@args);
  if ($nr->[0]{children}) {
    for my $ch(@{$nr->[0]{children}}) {
      return 1 if descendants_depth_last($self, $ch, $ffun, $level +1, $nid,@args);
    }
  }
  return 0 ;
}



sub old_get_node {
  my($self,$node)=@_;
  return () if !defined $self->{nodes}[$node];
  return $node;
}

sub descendants_depth_first {
  my ($self, $node, $ffun, $level)=@_;
  my $n = old_get_node($self,$node);
  my $nr = $self->{nodes}[$n];
  return 0 if (ref $nr ne 'ARRAY');
  if ($nr->[0]{children}) {
    for my $ch(@{$nr->[0]{children}}) {
      return 1 if descendants_depth_first($self, $ch, $ffun, $level +1);
    }
  }
  return 1 if $ffun->($n, $nr, $level);
  return 0 ;
}

sub derive_key {
  my ($self,$data)=@_;
  return graph_object::node::derive_key({}, $data);
}


=pod

sub derive_key {
  my ($self, $data, $n)=@_;
  return () if ref $data ne 'HASH';

  my $key;
  if (!defined ($key=$data->{key})) {
    if ($data->{node_type} eq 'url') {
      die (sprintf "node %s has no url:\n%s", $n, dumper($data))
        if !defined (my $url=$data->{url});
      $key = "url:$url";
    }
    elsif ($data->{node_type} eq 'request') {
      #my $req_node=$self->get_request()
      $key = "req:".$n;
    }
    else {
      die (sprintf "cannot derive a key for node %s\n%s", $n, dumper($data));
    }

  }
  return $key;
}

=cut

sub derive_data {
  my ($self,$node_set)=@_;
  my $nodes = $self->g->nodes // $self->g->nodes({});
  my $edges = $self->g->edges// $self->g->edges({});
  my $nodemap = $self->g->nodemap // $self->g->nodemap({});
  my $results = $self->results // $self->results({});
  my $nar = $self->{_nar};
  my $ear= $self->{_ear};
  my $ids={};
  my $warned;

  my $roots ={};
  my $seen_nodes={};
  my $data_keys = { map  {$_,1} qw(url query ts results node_type source rating comment)  } ;


  my $oldnode = sub  {
    my ($id,$nr,$level,$pid,$root)= @_;

    my $ndata = $nr->[0];
    if (ref $ndata ne 'HASH') {
      die sprintf "inconsistent data: %s, %s", $id, dumper($ndata);
    }

    if (!defined $pid)  {
      if (defined $seen_nodes->{$id}) {
        die "root $id already seen";
      }
      my $data=common::sub_hash($ndata, $data_keys);
      #my $key = $ndata->{key}//derive_key($self,$ndata,$id);


      graph_object::node::derive_data($data,$ndata->{key});
      #
      #my $okey = $ndata->{key};
      #if (defined $okey) {
      #  my ($url,$query,$topic);
      #  my $node_type= $ndata->{node_type};
      #  if ($node_type eq 'url' && !defined $ndata->{url}) {
      #    ($data->{url})=$okey =~ m{^url:(.*)};
      #  }
      #  elsif ($node_type eq 'query' && !defined $ndata->{query}) {
      #    ($data->{query})=$okey =~ m{^google:(.*)};
      #  }
      #  elsif ($node_type eq 'topic' && !defined $ndata->{topic}) {
      #    ($data->{topic})=$okey =~ m{^topic:(.*)};
      #  }
      #}
      #
      my $key ;
      eval { $key = $self->derive_key($data); };
      if ($@) {
        die sprintf "derive_key: %s, id=%s, data=%s", $@, $id, dumper($ndata);
      }
      $roots->{$id} = $seen_nodes->{$id} = { id=>$id, data=>$data } ; # must be traversed depth last

    }
    else {
      my $pref = $seen_nodes->{$pid};
      if (!defined $pref) {
        die sprintf "parent not found : id=%s, pid=%s,root=%s", $id,$pid,$root;
      }
      my $data=common::sub_hash($ndata, $data_keys);
      my $ch = $seen_nodes->{$id} = { id=>$id, parent=>$pid, root_id=>$root,  data=>$data};
      $roots->{$root}{descendants}{$id}=$ch;
      $pref->{children}{$id}=1;
    }
    0;
  };

  my $old_obj = { nodes => $nar};

  if ($nar && @$nar) {
    for (my $i=0;$i<@$nar;$i++ ) {
      my $nr = $nar->[$i];
      next if !defined $nr;
      #$maxid = $i if $maxid < $i;
      $self->set_id($i+1) if $self->get_id()<=$i;
      if ($seen_nodes->{$i}) {
        next;
      }
      my $nd = $nr ->[0];

      my $root = old_get_root($old_obj, $i); # old format

      if ($roots->{$root}) {
        die "root $root of node $i already seen";
      }
      descendants_depth_last($old_obj, $root, $oldnode, 0, undef, $root);
      my $meta = $roots->{$root};
      if (!defined ($meta)) {
        die sprintf "root node missing"
      }


      #if (!defined $meta->{key}) {
      #  my $key = $self->g->derive_key($meta->{data});
      #  if (!defined ($key )) {
      #    die "cannot derive key for new node";
      #  }
      #
      #  $meta->{key}=$key;
      #}

      if (defined $self->g->get_node_by_id($root)) {
        die "node id $root already exists";
      }
      $meta->{id}=$root;
      $self->set_id($root+1) if $self->get_id()<=$root;
			$meta->{partition}=$self->current;


      my $node=graph_object::node->new($meta);
      if (!defined $self->g->add_node($node) ) {
        die "cannot add new node";
      }
    }
    $self->changed(1);
  }

  if ($ear&& @$ear) {
    for (my $i=0 ;$i<@$ear; $i++ ) {
      my $er = $ear->[$i];
      next if !defined $er;

      my $from_child = $seen_nodes->{$er->[1]};
      my $to_child = $seen_nodes->{$er->[2]};

      if (!defined $from_child)  {
        die "node not found by id $er->[1]";
      }
      if (!defined $to_child)  {
        die "node not found by id $er->[2]";
      }
      my $fromi = old_get_root($old_obj, $er->[1]);
      my $toi = old_get_root($old_obj, $er->[2]);
      my ($from,$to);
      if (!defined (    $from = $seen_nodes->{$fromi})) {
        die "!defined (    my \$from = $seen_nodes->{$fromi})";
      }
      if (!defined (    $to = $seen_nodes->{$toi})) {
        die "!defined (    my \$to = $seen_nodes->{$toi})";
      }

      my $data = { edge_type=>$er->[0]};
      my $meta = {  data=>$data, from=>$from->id, to=>$to->id, id=>$self->next_id,
                  from_child=>$er->[1], to_child=>$er->[2], partition=>$self->current };
      #my $meta = { from=>$new_edge->[1]{node}, to=>$new_edge->[2]{node}
                    #, data=>{ edge_type=>$new_edge->[0]{edge_type}}, partition=>$self->current};


      my $edge=graph_object::edge->new($meta); #change to new($meta)

      if (!defined $self->g->add_edge($edge)) {
        die "cannot add new edge";
      }
    }
    $self->changed(1);
  }

  delete $self->{_nar};
  delete $self->{_ear};

  my $noderesults = sub {
    my ($id ,$node, $parent,$root, $level)=@_;

      #return 1 if $ffun->($node->{id}, $node, $parent, $root, $level, @args);
    	#$data->{results}{$request_id}= $result;


#  my $result = {id=>$request_id, type=>'result', order=>$order, url=>$url, title=>$title, text=>$text} ;
#	push @{$data->{results}{$request_id}}, $result;
#  my $i = @{$data->{results}{$request_id}}-1;
#  $self->results->{$url}{$node->id}{$request_id}=$i;

    if ($node->{data}{results}) {
      my $request_results = $node->{data}{results};
      my $new_results=[];
      if (ref $request_results eq 'ARRAY') {
        $new_results = $request_results
      }
      elsif (ref $request_results eq 'HASH') {
        for my $res (sort { $request_results->{$a}{order}<=>$request_results->{$b}{order}}  keys %$request_results) {
          push @$new_results, $request_results->{$res};
        }
        $node->{data}{results} = $new_results;
        $self->changed(1);
      }
      else {
        die sprintf "wrong data: ref request results eq %s, node %s, root %s "
        , ref $request_results, $node->{id}, $root->id
      }

      for (my $i=0;$i< @$new_results;$i++) {
        my $result = $new_results->[$i];
        if (!defined $result->{id}) {
          $result->{id}=$node->{id};
          $self->changed(1);
        }

        if ($result->{id} ne $node->{id}) {
          die sprintf "result_id %s!= node id %s, root %s", $result->{id}, $node->{id}, $root->id;
        }

        my $qresults = $self->results;
        my $urlres = $qresults->{$result->{url}};
        #$urlres->{$root->id}{$request_id}=$i;
        eval {
          $self->results->{$result->{url}}{$root->id}{$result->{id}}=$i;
        };
        if ($@) {
          die $@;
        }
      }
    }
  };

  my $nextid =$self->get_id();

  for my $nid (keys %$nodes) {
    if ($nid >= $nextid ) {
      $nextid= $nid+1;
    }

    my $node = $self->g->get_node_by_id($nid) ;
    if ($node->{descendants} ) {
      for my $id (keys %{$node->{descendants}}) {
        if ($id >= $nextid ) {
          $nextid= $id+1;
        }
      }
    }


    if (ref $node ne 'graph_object::node') {
      bless ($node, 'graph_object::node');
    }
    if (!$node) {
      die sprintf "node not defined: node id %s", $node->id
    }

    my ($key,$derived) = $node->key;
    if (!defined $key) {
      die sprintf "key not defined: node id %s", $node->id
    }



    if ($derived eq 'old' ) {
      my $dkey;
      eval { $dkey = $node->derive_key() };
      if ($@) {
        my $data = $node->{data};
        my $okey = $node->{key}//$node->{data}{key};
        $self->derive_node_data($data,$okey);
      }
      $dkey = $node->derive_key();
      delete $node->{key};
      delete $node->{data}{key};
      if ( $dkey ne $key) {
        $self->changed(1);
        $key =$dkey;
      }
    }
    else {
      my $data= $node->{data};
      if (my $t = $data->{relevance}) {
        if ($t->{sample}) {
          delete $t->{sample};
          $self->changed(1);
        }
      }
    }

    if (defined $node->{storage} ) {
      if (!defined $node->{partition}) {
        if (!$warned++) {
          warn sprintf "node %s still has a storage instead of a partition\n", $node->id;
        }
        $node->{partition}=$node->{storage};
      }
      delete $node->{storage};
      $self->changed(1); # TODO: keeping track of changed partitions
    }

    my $tid = $self->g->{key2n}{$key} ;
    if (defined $tid) {
      if ($tid ne $node->id) {
        die sprintf "node keys mismatched: %s<>%s, key %s", $node->id, $tid, $key;
      }
    }
    else {
      $self->g->{key2n}{$key}=$node->id;
    }
    $self->g->{n2key}{$node->id}=$key;
      #my ($node, $parent, $root, $ffun, $level, @args)=@_;
    $node->descend_dl(undef, $node, $noderesults,0);
  }

  for my $eid (keys %$edges) {
    if ($eid >= $nextid ) {
      $nextid= $eid+1;
    }
    my $edge = $self->g->get_edge_by_id($eid) ;
    bless ($edge, 'graph_object::edge');
  }

  $self->set_id($nextid);

  0;
};

sub rebless {
  my ($self)=@_;
  my $cn;
  for my $nid (keys %{$self->g->nodes}) {
    my $node = $self->g->get_node_by_id($nid) ;

    if (ref $node ne 'graph_object::node') {
      bless ($node, 'graph_object::node');
      $cn++;
    }
  }
  my $ce;
  for my $eid (keys %{$self->g->edges}) {
    my $edge = $self->g->get_edge_by_id($eid) ;
    if (ref $edge ne 'graph_object::edge') {
      bless ($edge, 'graph_object::edge');
      $ce++;
    }
  }

  #printf STDERR "reblessed: %s %s\n", $cn, $ce;
}


1;
;
