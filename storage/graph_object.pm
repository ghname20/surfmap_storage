#!perl
package graph_object;
{
use strict;
use Data::Dumper;
use Data::Compare;
#use Moose;

=pod
has 'verbose' => ( is => 'rw', isa =>'Int', default=>1 );
has 'ignore_dups' => ( is => 'rw', isa =>'Int', default=>1 );
has 'debug' => ( is => 'rw', isa =>'Int', default=>0 );
has 'junk' => ( is => 'rw', isa =>'Str', default=>'rw' );
has 'core' => ( is => 'rw', isa =>'Str', default=>'rw' );
has 'max_edges' => ( is => 'rw', isa =>'Int', default=>300 );
has 'max_nodes' => ( is => 'rw', isa =>'Int', default=>300 );
has 'defragment' => ( is => 'rw', isa =>'Int', default=>1 );

=cut

my $default_cfg= {
  verbose=>1,
  defragment=>0,
  max_nodes=>300,
  max_edges=>300,
  core=>'rw',
  junk=>'rw',
  debug=>0,
  ignore_dups=>1,
};

sub default_cfg {
  return $default_cfg;
}


sub new {
	my ($class, $cfg)=@_;
	$cfg={ %$default_cfg, %{$cfg//{}} };
  #my $self=bless { cfg=>$cfg, requests=>0, next_id=>0, edges=>{}, nodes=>{}} ;
  my $self=bless { cfg=>$cfg, requests=>0, edges=>{}, nodes=>{}} ;
  return $self;
}


sub nodes  {
  my ($self,$nodes)=@_;
  return $self->{nodes}=$nodes if (defined $nodes );
  return $self->{nodes};
}

sub edges  {
  my ($self,$edges)=@_;
  return $self->{edges}=$edges if (defined $edges );
  return $self->{edges};
}



sub key2n  {
  my ($self,$key2n)=@_;
  return $self->{key2n}=$key2n if (defined $key2n );
  return $self->{key2n};
}

#n2key
sub n2key  {
  my ($self,$n2key)=@_;
  return $self->{n2key}=$n2key if (defined $n2key );
  return $self->{n2key};
}

sub get_data {
	my ($self,$node)=@_;
  return () if !defined ($node = $self->get_node($node));
  return () if 'ARRAY' ne ref (my $nr = $self->{nodes}[$node]);
  return wantarray ? ($nr->[0],$nr, $node) : ($nr->[0]);
}

sub get_noderef {
	my ($self,$node)=@_;
  return () if !defined ($node = $self->get_node($node));
  return () if 'ARRAY' ne ref (my $nr = $self->{nodes}[$node]);
  return wantarray ? ($nr, $node) : $nr;
}


sub nodemap {
  my ($self,$p)=@_;
  $self->{nodemap}=$p if $p ;
  return $self->{nodemap};
}



=pod
sub get_fixed_node {
	my ($self,$key)=@_;
  unless (my $node=$self->nodes->{$key} ) {
    return ();
  }
  if (ref $node ne 'graph_object::node') {
    return () if (ref $node ne 'ARRAY') ;
    return bless ($node, 'graph_object::node');
  }
  return $node;
}
=cut

sub get_node_by_key {
	my ($self,$key)=@_;
  my $id  = $self->{key2n}{$key};
  return $self->get_node_by_id($id);
}

sub get_node_by_id{
	my ($self,$id)=@_;
  return $self->{nodes}{$id};
}

sub get_key_by_id {
  my ($self,$id)=@_;
  my $t;
  if (defined ($t=$self->{n2key}{$id})) {
    return $t;
  }
  my $node= $self->get_node_by_id($id);
  if (!defined $node) {
    die sprintf "node not found by id $id";
  }
  return $node->key;
}


sub get_edge_by_id{
	my ($self,$id)=@_;
  return $self->edges->{$id};
}


#sub get_edge_by_peers {
#	my ($self,$from,$to)=@_;
#  return $self->{id2e}{$id};
#}


=pod

sub get_or_add_node {
  my ($self, $key, $data)=@_;
	my $node =get_node($self,$key) ;
	if (!$node) {
    $data->{key} = $key;
		$node = add_node($self,$data,$key);
	}
	$self->{nodes}->[$node][0]=$data;

  return $node;
}


=cut

sub add_node {
	my ($self,$node, $partition)=@_;
  my $key = $node->key;
  my $id  = $node->id;
  if (!defined $id) {
    die "node id not defined";
  }
  my $nid = $id;
  #my $nid = $id // $self->next_id;
  if ($self->{key2n}{$key}) {
    die "node key $key already exists";
  }
  if ($self->nodes->{$nid} ) {
    die "node id $nid already exists";
  }
  $self->nodes->{$nid} = $node;
  $self->{key2n}{$key} = $nid;
  $self->{n2key}{$nid} = $key;
  if (!defined $id) {
    $node->id($nid);
  }

  if ($node->{edges}) {
    $node->{edges}= Clone::clone($node->{edges});
  }

	$node->{partition}=$partition if defined $partition;
	return wantarray ? ($nid,$node):$nid;
}


sub change_key {
	my ($self,$node,$key)=@_;
  my $okey = $node->key;
  my $nid = $node ->id;
  return () if $key eq $okey;
  if ($self->get_node_by_key($key)) {
    die sprintf "the key %s already exists\n", $key;
  }
  delete $self->{key2n}{$okey};
  $self->{key2n}{$key}=$nid;
  $self->{n2key}{$nid}=$key;
}

sub delete_node {
  my ($self,$id)=@_;
  my $node = $self->get_node_by_id($id);
  if (!$node) {
    die "node with id '$id' does not exist";
  }
	for my $eid (keys %{$node->edges}) {
    if ($self->get_edge_by_id($eid)) {
      $self->delete_edge($eid)
    }
	}
  delete $self->key2n->{$node->key};
	delete $self->nodes->{$node->id};
  delete $self->n2key->{$node->id};


  $self->{changed}++;
  #$self->defragment();
  #if ($self->consistency_check()) {
  #  print STDERR "node $node deleted consistenly\n";
  #}
  return 1;
}


=pod
  for my $i (sort keys %$node_set) {
    next if !defined (my $nr = $nodes->[$i]);
    descendants_depth_first($self,$i,$proc);
  }
=cut


sub consistency_check {
  my ($self,$args)=@_;
  $args//={check_nodes=>1, check_links=>1};
  my $nodes= $self->nodes;
  my $edges= $self->edges;
  my (%seen_from, %seen_to, %children_actual, %children_expected);
  #for (my $i=0;$i<@$nodes;$i++) {
  for my $nid (sort keys %{$self->nodes}) {
    next if !defined (my $node = $nodes->{$nid});
    if (ref $node ne 'graph_object::node' ) {
      die sprintf "ref node[$nid] ne 'graph_object::node'-----\n%s", dumper($node);
    }
    my $data = $node->data;
    if (ref $data ne 'HASH' ) {
      die sprintf "ref data{$nid} ne 'HASH'-----\n%s", dumper($node);
    }
    my $edges_from = $node->edges;
    #my $to = $noderef->[2];
    if (ref $edges_from ne 'HASH' ) {
      die sprintf "ref edges_from[$nid] ne 'HASH'-----\n%s", dumper($node);
    }
    #if (ref $to ne 'ARRAY' ) {
    #  die sprintf "ref to[$nid] ne 'ARRAY'-----\n%s", dumper($noderef);
    #}
    for my $eid (keys %$edges_from) {
      my $edge = $self->get_edge_by_id($eid);
      #my $linkref = $links->[$i_link];
      if (ref $edge ne 'graph_object::edge' ) {
        die sprintf "ref node{$nid}->edge{$eid} ne 'graph_object::edge'-----\n%s\n------\n%s", dumper($node), dumper($edge);
      }
      if ($edge->from ne $nid && $edge->to ne $nid) {
      #if ($linkref->[2] != $i) {
        die sprintf "incorrect backlink: node{$nid}->edge{$eid}={%s->%s}-----\n%s\n------\n%s"
          , $edge->from,$edge->to, dumper($node), dumper($edge);
      }
      #$seen_from{$i_link}++;
    }

    #for my $eid(@$to) {
    #  my $linkref = $links->[$i_link];
    #  #next if !defined $linkref;
    #  if (ref $linkref ne 'ARRAY' ) {
    #    die sprintf "to[$i]->link[$i_link] ne 'ARRAY'-----\n%s\n------\n%s", dumper($noderef), dumper($linkref);
    #  }
    #  if ($linkref->[1] != $i) {
    #    die sprintf "incorrect backlink: to[$i]->link[$i_link]!=$linkref->[1]-----\n%s\n------\n%s", dumper($noderef), dumper($linkref);
    #  }
    #  $seen_to{$i_link}++;
    #}
    ##my $parent = $data->{parent};


=pod
    if (my $p = $self->get_node($data->{parent})) {
      push @{$children_actual{$p}}, $i;
    }

    next unless my $children= $data->{children};
    $children_expected{$i}=@$children;
    for my $ch_key (@$children) {
      my ($chdata,$chref,$ch)= $self->get_data($ch_key);
      if (!$chdata) {
        die sprintf "child does not exist: data[$i]->$ch_key-----\n%s", dumper($noderef);
      }
      if (get_node($self,$chdata->{parent}) ne $i) {
        die sprintf "child has wrong parent: {$ch_key}[$ch]\->$chdata->{parent}!=$i-----\n%s\n------\n%s", dumper($noderef), dumper($chref);
      }
      #if ($data->{parent}) {
      #  push @{$children_actual{$i}}, $ch;
      #}
    }
=cut
  }

  for my $eid (sort keys %$edges) {
    my $edge = $self->get_edge_by_id($eid);
    if (!$edge) {
      die sprintf "no edge '$eid' in edges";
    }
    my $from = $self->get_node_by_id($edge->from);
    if (!$from) {
      die sprintf "no from node '%s': edge %s", $edge->from, dumper($edge);
    }
    if (!exists $from->edges->{$eid}) {
      die sprintf "node '%s' has no backlink: edge %s", $from->id, dumper($edge);
    }
    my $to = $self->get_node_by_id($edge->to);
    if (!$to) {
      die sprintf "no to node '%s': edge %s", $edge->to, dumper($edge);
    }
    if (!exists $from->edges->{$eid}) {
      die sprintf "node '%s' has no backlink: edge %s", $to->id, dumper($edge);
    }
  }
=pod
  for (my $i_link =0;$i_link<@$links;$i_link++) {
    if ($seen_from{$i_link}!=1 ||$seen_to{$i_link}!=1) {
      next if $seen_from{$i_link}==0 && $seen_to{$i_link}==0 && !defined$links->[$i_link];
      die sprintf "broken or duplicated link: [$i_link] seen times: from=%d, to=%d\n-----\n%s"
      , $seen_from{$i_link}, $seen_to{$i_link}, dumper($links->[$i_link]);
    }
  }

=cut

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
              ,  dumper($nodes->[$parent][0]);
    }
  }
=cut

  return 1;
}
sub move_link { #unsafe, slow
  my ($self,$i_from,$i_to)=@_;
  my $links =$self->{links};
  my ($from, $to);
  if (!defined ($from = $links->[$i_from]) || defined $links->[$i_to]) {
    die "wrong link move: [$i_from]=$links->[$i_from], [$i_to]=$links->[$i_to]";
  }
  my $in_links =  $self->{nodes}[$from][1];
  my $out_links =  $self->{nodes}[$from][2];
  for (my $i=0; $i<@$in_links; $i++) {
    if ($in_links->[$i]==$i_from) {
      $in_links->[$i]=$i_to;
    }
  }
  for (my $i=0; $i<@$out_links; $i++) {
    if ($out_links->[$i]==$i_from) {
      $out_links->[$i]=$i_to;
    }
  }
  $links->[$i_to]=$links->[$i_from];
  delete $links->[$i_from];
}


=pod
sub delete_link {
  my ($self,$i_link)=@_;
  my $links =$self->{links};
  my $nodes = $self->{nodes};
  return () if !defined (my $link = $links->[$i_link]);
  my ($i_from, $i_to) = @{$links->[$i_link]}[1,2];
  my ($from, $to)=@$nodes[$i_from, $i_to];
  if (ref $from ne 'ARRAY' || ref $to ne 'ARRAY') {
    die sprintf "found broken link: link[$i_link]=%s: [$i_from]->[$i_to]----\n%s\t-- -> --\n%s\n"
      , dumper($link), dumper($from), dumper($to);
  }

  @{$from->[2]} = grep { $_!=$i_link }  @{$from->[2]};
  @{$to->[1]} = grep { $_!=$i_link }  @{$to->[1]};

  delete $links->[$i_link];

  return 1;
}
=cut

sub defragment {
  my ($self)=@_;
  $self->defragment_nodes();
  $self->defragment_links();
  $self->rebuild_back_links();

}

sub defragment_links {
  my ($self)=@_;

  my $links =$self->{links};
  my $next=0;
  for (my $i=0;$i<@$links; $i++) {
    if (defined $links->[$i]) {
      if ($next < $i) {
        $links->[$next]=$links->[$i]
      }
      $next++;
    }
  }
  $#$links=$next-1;
}



sub switch_links_incst {
  my ($self, $from, $to)=@_;
  my $fromref= $self->{nodes}[$from];
  return () if !defined $fromref;
  my (@src,@dst);
  for my $i_link(@{$fromref->[1]}) {
    next if !defined (my $e = $self->{links}[$i_link]);
    die "inconsistent source: node=$from, edge=$i_link"
      if $e->[2]!=$from;
    $e->[2] = $to;
    push @src, $i_link;
  }
  for my $i_link(@{$fromref->[2]}) {
    next if !defined (my $e = $self->{links}[$i_link]);
    die "inconsistent target: node=$from, edge=$i_link"
      if $e->[1]!=$from;
    $e->[1] = $to;
    push @dst, $i_link;
  }
  return (\@src,\@dst);
}


sub move_node {
  my ($self,$i_from, $i_to)=@_;
  my $nodes =$self->{nodes};
  my $links =$self->{links};
  die "wrong node move: [$i_from]=$nodes->[$i_from] => [$i_to]=$nodes->[$i_to]"
    if (defined $nodes->[$i_to] || !defined $nodes->[$i_from]);
  my $noderef = $nodes->[$i_from];
  my $from_links = $noderef->[1];
  my $to_links = $noderef->[2];

  for my $i_link (@$from_links) {
    my $linkref  = $links->[$i_link];
    if ($linkref->[2] != $i_from) {
      die sprintf "link source data inconsistent: args=(%s,%s), \$links->[%s]=%s", $i_from, $i_to, $i_link, dumper($linkref);
    }
    $linkref->[2]=$i_to;
  }
  for my $i_link (@$to_links) {
    my $linkref  = $links->[$i_link];
    if ($linkref->[1] != $i_from) {
      die sprintf "link destination data inconsistent: args=(%s,%s), \$links->[%s]=%s", $i_from, $i_to, $i_link, dumper($linkref);
    }
    $linkref->[1]=$i_to;
  }

=pod
  if (my $chs=$noderef->[0]{children}) {
    for my $ch(@$chs) {
      if ((my $p =($nodes->[$ch][0]{parent}//-1)) !=$i_from) {
        die sprintf "while moving node %d: wrong parent %d of child %d", $i_from, $p, $ch;
      }
      $nodes->[$ch][0]{parent}=$i_to;
    }
  }
=cut

=pod
  if (my $p=$noderef->[0]{parent}) {
    my $pchs;
    my $pr= $nodes->[$p];
    if (!defined $pr) {
      die "wrong parent $p of node $i_from";
    }
    if ($noderef->{node_type} eq 'request')  {
      if (!defined $pr->{key}) {
        die "parent $p of request node $i_from has no key";
      }
      if (!defined (my $ts = $noderef->{ts})) {
        die "request node $i_from has no timestamp";
      }
      my $req_node=  $self->{request_map}{$pr->{key}} && $self->{request_map}{$pr->{key}}{$ts};
      if ($req_node ne $i_from) {
        die "request node $i_from has different mapping: $pr->{key}:$ts->$req_node";
      }
      $self->{request_map}{$pr->{key}}{$ts} = $i_to;
    }
    unless ($pchs=$pr->[0]{children}) {
      die sprintf "while moving node %d: parent %d has no children", $i_from, $p;
    }
    my $ok;

    $self->{request_map}{$noderef->{key}}{$nodes->[$ch][0]{ts}=
    for my $pch(@$pchs ) {
      do { $pch=$i_to; $ok++;} if ($pch == $i_from) ;

    }
    if ($ok!=1) {
      die sprintf "while moving node %d: parent %d had %d references to the node", $i_from, $p, $ok;
    }
  }
=cut

  $nodes->[$i_to] = $nodes->[$i_from];
  $self->{nodemap}{$nodes->[$i_to][0]{key}} = $i_to;
  delete $nodes->[$i_from];


}

sub defragment_nodes {
  my ($self)=@_;
  my $nodes =$self->{nodes};
  my $next=0;
  for (my $i=0;$i<@$nodes;$i++) {
    my $node = $nodes->[$i];
    if (defined $node) {
      if ($next < $i ) {
        $self->move_node($i,$next);
      }
      $next++;
    }
  }
  $#$nodes = $next-1;
}

=pod
sub rebuild_back_links {
  my ($self,$broken)=@_;
  my $nodes= $self->{nodes};
  my $links =$self->{links};
  my ($from,$to)=([],[]);
  for (my $i=0;$i<@$links; $i++) {
    next if !defined $links->[$i];
    push @{$from->[$links->[$i][1]]}, $i;
    push @{$to->[$links->[$i][2]]}, $i;
  }

  if ($broken) {
    for (my$i=0;$i<@$broken;$i++) {
      my $noderef= $nodes->[$i];
      next if !$noderef;
      if (!$noderef || ref $noderef ne 'ARRAY' || ref $noderef->[0] ne 'HASH') {
        die sprintf "node %s corrupted: %s", $i, dumper($noderef);
      }
      $noderef->[2]=$from->[$i] || [];
      $noderef->[1]=$to->[$i] || [] ;
    }
  }
  else {
    for (my$i=0;$i<@$nodes;$i++) {
      my $noderef= $nodes->[$i];
      next if !$noderef;
      if (!$noderef || ref $noderef ne 'ARRAY' || ref $noderef->[0] ne 'HASH') {
        die sprintf "node %s corrupted: %s", $i, dumper($noderef);
      }
      $noderef->[2]=$from->[$i] || [];
      $noderef->[1]=$to->[$i] || [] ;
    }
  }
}

=cut

=pod


sub delete_node_child {
  my ($self,$args)=@_;
  my $nodes= $self->{nodes};
  my $op = $args->{operation}//'delete';
  if ($op ne 'delete') {
    die "wrong operation '$op'";
  }
	my $key = ($args->{key}//="url:$args->{url}");
  my $node = $self->get_node_by_key($key);
  if (!$node) {
    die "node '$key' does not exist";
  }
	my $child = $args->{child};
	$node->delete_child($child);
	if ($self->g->) {
		#code
	}



  my $data = $self->get_data($node);
  if ($data->{parent}) {
    $self->remove_from_parent($node);
  }
  $self->delete_node_recursive($node);
  $self->{changed}++;
  $self->defragment();
  #if ($self->consistency_check()) {
  #  print STDERR "node $node deleted consistenly\n";
  #}
  return 1;
}

=cut




sub delete_node_incst_recursive {
  my ($self,$node)=@_;
  my $noderef =  $self->{nodes}[$node];
  $self->{broken}=1;
  #my @del = @{$noderef->[1]};
  #for my $from(@del) {
  ##  printf STDERR "from: deleting %s of %s\n", $lc++, 0+@{$noderef->[1]};
  #  $self->delete_link($from);
  #}
  #@del = @{$noderef->[2]};
  #for my $to(@del) {
  # # printf STDERR "to: deleting %s of %s\n", $lc++, 0+@{$noderef->[2]};
  #  $self->delete_link($to);
  #}
  if (my $chs=$noderef->[0]{children}) {
    for my $ch (@$chs)  {
      $self->delete_node_incst_recursive($ch);
    }
  }

  my $key =  $noderef->[0]{key};
  delete $self->{nodemap}{$key};
  delete $self->{nodes}[$node] ;
}

sub delete_node_recursive {
  my ($self,$node)=@_;
  my $noderef =  $self->{nodes}[$node];
  my $lc=1;
  my @del = @{$noderef->[1]};
  for my $from(@del) {
  #  printf STDERR "from: deleting %s of %s\n", $lc++, 0+@{$noderef->[1]};
    $self->delete_link($from);
  }
  $lc=1;
  @del = @{$noderef->[2]};
  for my $to(@del) {
   # printf STDERR "to: deleting %s of %s\n", $lc++, 0+@{$noderef->[2]};
    $self->delete_link($to);
  }
  if ($noderef->[0]{children}) {
    $self->delete_children($node);
  }

  my $key =  $noderef->[0]{key};
  delete $self->{nodes}[$node] ;
  delete $self->{nodemap}{$key};
}

sub get_node_info {
  my ($self, $args)=@_;
  my $key = $args->{key}// "url:$args->{url}";
  my $url_node = get_node($self,$key);
  if (!$url_node) {
    die "not found: $key";
  }
  my $data= get_data($self, $url_node);
  #my $json = $self->get_graph_data ($args);

  my $rv = { %$args,
    rating => $data->{rating},
    comment => $data->{comment},
  };
  if ($rv->{node_type} eq 'url' && $rv->{url} eq '') {
    $rv->{url}=$key=~s{^url:}{}r;
  }
  return $rv;
}


sub get_collapsed_node {
  my ($self,$node,$seen)=@_;

  $seen//={};
  my $from=[];
  my $to = [];
  my $data={};

  my $node_fn = sub {
    my($node, $nr, $level) = @_;
    $seen->{$node}++;
    push @$from, @{$nr->[1]};
    push @$to, @{$nr->[2]};
    for my $k (keys %{$nr->[0]}) {
      push @{$data->{$k}}, $nr->[0]{$k};
    }
    return 0 ;
  };
  descendants_depth_last($self,$node,$node_fn);
  return [$data,$from,$to];
}

#returns hash of node indexes
sub get_roots {
  my ($self)=@_;
  my $nodes=$self->{nodes};
  my $roots={};
  my $node2root={};
  my $cnt;
  my $current_root;
  my $flt = sub {
    my ($node,$nr,$level)=@_;
    $node2root->{$node}=$current_root;
    ++$cnt;
    0;
  };
  for (my$i=0;$i<@$nodes;$i++) {
    next if $node2root->{$i};
    #next unless defined (my $nr =$nodes->[$i]);
    next if !defined $nodes->[$i];
    $current_root=get_root($self, $i);
    $cnt=0;
    $self->descendants_depth_last($current_root, $flt);
    $roots->{$current_root}=$cnt;
  }
  return wantarray? ($roots, $node2root) : $roots;
}

sub get_collapsed_roots {
  my ($self)=@_;
  my $nodes=$self->{nodes};
  my $roots = $self->get_roots();
  my $collapsed = {};
  for my $root(keys %$roots ) {
    next unless defined (my $nr =$nodes->[$root]);
    $collapsed->{$root} = get_collapsed_node($self, $root);
  }
  return $collapsed;
}


sub graph_stats {
  my ($self)=@_;
  my $nodes=$self->{nodes};
  my $edges=$self->{links};
  my $stats={ empty_nodes=>0, isolated_nodes=>0, empty_edges=>0  };
  my $scratch ={};
  my $seen={};
  my $collapsed = $self->get_collapsed_roots();
  for (my$i=0;$i<@$nodes;$i++) {
    next unless defined (my $nr =$nodes->[$i]);
    if (@$nr==0) {
      $stats->{empty_nodes}++;
      next;
    }
    $stats->{node_type}{$nr->[0]{node_type}}++;
    if (@{$nr->[1]}==0 && @{$nr->[2]}==0) {
      $stats->{isolated_nodes}++;
    }
    if (my $rootc = $collapsed->{$i}) {
      if (!@{$rootc->[1]} && !@{$rootc->[2]}) {
        $stats->{isolated_tree}++;
      }
    }
  }
  for (my $i=0;$i<@$edges;$i++) {
    next unless defined (my $er =$edges->[$i]);
    if (@$er==0) {
      $stats->{empty_edges}++;
      next;
    }
  }
  return wantarray? ($stats,$scratch) : $stats;
}

sub get_stats {
  my ($self,$edges)=@_;
  my $seen_nodes={};
  my ($stats)={};
  for my $eid (keys %$edges) {
		my $edge=$self->get_edge_by_id($eid);
    #my $linkr = $self->{links}[$edge];
    #if ($linkr->[1] ne $er->[1] || $linkr->[2] ne $er->[2]) {
    #  die "filtered link data inconsistent";
    #}
    if (!$seen_nodes->{$edge->from}++) {
      $stats->{nodes}++;
    }
    if (!$seen_nodes->{$edge->to}++) {
      $stats->{nodes}++;
    }
    $stats->{edges}++;
  }
  return $stats;
}

=pod
sub get_stats3 {
  my ($self,$edges)=@_;
  my $seen_nodes={};
  my ($stats)={};
  for my $edge (values %$edges) {
#    my ($i_link, $i_from, $i_to) = ($edge ,@{$self->{links}[$edge]}[1,2]);
    my ($i_link, $i_from, $i_to) = @$edge;
    #my ($i_link, $i_from, $i_to) = @$edge;
    #my $linkr = $self->{links}[$edge];
    #if ($linkr->[1] ne $er->[1] || $linkr->[2] ne $er->[2]) {
    #  die "filtered link data inconsistent";
    #}
    if (!$seen_nodes->{$i_from}++) {
      $stats->{nodes}++;
    }
    if (!$seen_nodes->{$i_to}++) {
      $stats->{nodes}++;
    }
    $stats->{edges}++;
  }
  return $stats;
}


=cut


#sub flatten_graph {
#  my ($self, $edges, $map, $ffun)=@_;
#  my $pairs = [ map { $self->{links}[$_]} $edges ]
#
#

=pod

sub flatten_edges {
  my ($self,$edges, $map, $ffun)=@_;
  my $links = $self->{links};
  my $nodes= $self->{nodes};
  my $edges_out = {};

  $ffun //= sub {
      my ($n,$nr)= @_; for my $o (@$order ) {
            if ($nr->[0]{node_type} eq $o || $nr->[0]{$o} ) {return 'node'}
          }
      return ();
     };

  for my $i_link (@$edges) {
    my $linkref = $links->[$i_link];
    my ($i_from, $i_to) = @$linkref[1,2];
    #my $fromref = $nodes->[$linkref->[1]];
    #my $toref = $nodes->[$linkref->[2]];
    my $traits=get_farthest_ancestors($self, $i_from, $ffun);
    my $from = $traits->{node};
    $traits=get_farthest_ancestors($self, $i_to, $ffun);
    my $to=$traits->{node};
    next unless ($from && $to );
    #next if $map->{$from}{$to};
    $map->{$from}{$to}{$i_link}++;
    $edges_out->{$i_link} = [$i_link, $from, $to];

     #my ($fromroot) = get_farthest_ancestor($self, $from) || next;
     #my ($toroot) = get_farthest_ancestor($self,$to) || next;

     #my ($t);
     #my $fromnode  = $from_traits->{topic} || $from_traits->{query} || $from_traits->{url};
     #my $tonode = $to_traits->{topic} || $to_traits->{query} || $to_traits->{url};
  }
  return $edges_out;
}

=cut


sub has_edges {
  my ($self, $node, $level)=@_;
  my $has_edges=0;
  my $ffun = sub {
    my ($node, $nr, $level)=@_;
    if (@{$nr->[1]} || @{$nr->[2]}) {
      return ++$has_edges;
    }
  };
  descendants_depth_last($self, $node, $ffun);
  return $has_edges;
}

#input: $nid = node id , $ffun = filter sub
#output: $ret_flt_edges=hash {{edge id}=>cnt}, $ret_seen_nodes = hash {{node_id}=>cnt,...}

sub get_subgraph_recursive {
    my ($self, $nid, $ffun, $ret_flt_edges, $ret_seen_nodes)=@_;
    #my $root_node = $self->get_root($orig_node);

    if ($ret_seen_nodes->{$nid}++) {
      return ();
    }
    my $node = $self->get_node_by_id($nid);
		if (!defined $node) {
			die "node with id $nid does not exist";
		}

    while(my($eid,$peer_id)=each %{$node->edges}) {
			my $edge =$self->get_edge_by_id($eid);
			if (!defined $edge) {
				die "edge with id $eid does not exist";
			}
		  #my $lr = $self->edges}[$link];
      if ($ffun->($node,$edge,$peer_id eq $edge->from ? 'in' : 'out')) {
        $ret_flt_edges->{$eid}++;
        $self->get_subgraph_recursive( $peer_id ,$ffun, $ret_flt_edges, $ret_seen_nodes);
      }
    }
}

sub old_get_subgraph_recursive {
    my ($self, $orig_node, $seen, $flt_links, $ffun)=@_;
    #my $root_node = $self->get_root($orig_node);

    if ($seen->{$orig_node}++) {
      return ();
    }
    #$seen->{$orig_node}++;
    #my @out;
    my $nr = $self->{nodes}[$orig_node];
    for my $link (@{$nr->[1]}) {
      #next if ($seen->{$link});
      my $lr = $self->{links}[$link];
      if ($ffun->($link,$lr,0)) {
        #push @out, $link;
        $flt_links->{$link}++;
        #$seen->{$link}++;
        #push @out, get_subgraph_recursive($self, $lr->[1], $seen, $ffun);
        get_subgraph_recursive($self, $self->get_root($lr->[1]), $seen, $flt_links ,$ffun);
      }
    }
    for my $link (@{$nr->[2]}) {
      #next if ($seen->{$link});
      my $lr = $self->{links}[$link];
      if ($ffun->($link,$lr,1)) {
        $flt_links->{$link}++;
  #      push @out, $link;
        #$seen->{$link}++;
        get_subgraph_recursive($self, $self->get_root($lr->[2]), $seen, $flt_links ,$ffun);
        #push @out, get_subgraph_recursive($self, $lr->[2], $seen, $ffun);
      }
    }
    if ('ARRAY' eq ref $nr->[0]{children} ) {
      for my $child (@{$nr->[0]{children}}) {
        get_subgraph_recursive($self, $child, $seen, $flt_links ,$ffun);
      }
    }

  #return @out;
}



#input : $nodes=hash { {node_id}=>1 ...}, $ffun filter subm
#output: $ret_seen = hash {{node_id}=>cnt,...}, $ret_flt_edges=hash {{edge id}=>cnt}

sub get_subgraph {
  my ($self, $nodes, $ffun, $ret_flt_edges, $ret_seen_nodes)=@_;
  $ret_seen_nodes//={};
  $ret_flt_edges//={};
  my @out;
  for my $nid ( keys %$nodes) {
    #my ($self, $nid, $seen, $ret_flt_edges, $ffun)=@_;

    $self->get_subgraph_recursive($nid,$ffun,$ret_flt_edges, $ret_seen_nodes);
    #push @out, $self->get_subgraph_recursive($node,$seen,$ffun);
  }
  return ($ret_flt_edges, $ret_seen_nodes) ;
}


#input : $fnodes=hash { {node_id}=>1 ...}
#output: $ret_graph_updown= hash {{edge id}=>cnt},
sub get_subtree {
  my ($self, $fnodes)=@_;
  my $nodes = $self->nodes;
  my $edges = $self->edges;
  my $fromseeds = [];
  my $toseeds = [];

  my ($ret_graph_up,$ret_seen_nodes_up)=({},{});

  sub ffun1 {     my ($node, $edge, $dir)=@_; return $dir eq 'in'   };
  sub ffun2 {     my ($node, $edge, $dir)=@_; return $dir eq 'out'   };
  $self->get_subgraph( $fnodes, \&ffun1, $ret_graph_up,$ret_seen_nodes_up);

  my ($ret_graph_updown, $ret_seen_nodes_down) = ($ret_graph_up, {}) ;
  $self->get_subgraph( $fnodes, \&ffun2, $ret_graph_updown, $ret_seen_nodes_down);

  return $ret_graph_updown;
}



sub get_subtree_from_edges {
  my ($self, $flinks)=@_;
  my $nodes = $self->{nodes};
  my $links = $self->	{links};
  my $fromseeds = [];
  my $toseeds = [];
  for (my $i=0;$i<@$flinks; $i++) {
    my $link = $flinks->[$i];
    push @$fromseeds, $link->[1];
    push @$toseeds, $link->[1];
  }
  my @uptree = get_subgraph($self, $fromseeds, sub {
    my ($link, $lref, $dir)=@_; return $dir==0
  });
  my @downtree = get_subgraph($self, $toseeds, sub {
    my ($link, $lref, $dir)=@_; return $dir==1
  });

  return [@uptree, @downtree];
}



sub find_peers {
  my ($self, $edge_type,$i_from,$i_to, $limit)=@_;
  my $from = $self->{nodes}[$i_from];
  my $to= $self->{nodes}[$i_to];
  return () if ref $from ne 'ARRAY' || ref $to ne 'ARRAY';
  my @result;
  my $i =0;
  for my $t (@{$from->[2]}) { # TODO: check back link consistency
    my $link = $self->{links}[$t];
    if ($link->[2] == $i_to && ( !defined $edge_type  || $edge_type eq $link->[0])) {
      push @result, $link;
    }
    last if $limit && ++$i >= $limit;
  }
  return @result;
}

sub find_edges {
  my ($self, $filter, $limit)=@_;
	my ($from,$to);
	my $found=[];
	if (defined $filter->{from}) {
		$from=$self->get_node_by_id($filter->{from});
		return $found if !defined $from;
	}
	if (defined $filter->{to}) {
		$to=$self->get_node_by_id($filter->{to});
		return $found if !defined $to;
	}
	if (defined $from) {
		for my $eid(sort keys %{$from->edges} ) {
			my $edge=$self->get_edge_by_id($eid);
      next unless $edge->from eq $from->id  && (!defined ($to) || $edge->to eq $to->id);
			next if defined ($filter->{id}) && $filter->{id} ne $edge->id;
			if (!defined $filter->{data}) {
				push @$found, $edge;
				next;
			}
			my$t;
			next if defined ($t=$filter->{data}{edge_type}) && $t  ne $edge->data->{edge_type};
			push @$found, $edge;
      last if @$found >=$limit;

		}
	}
	elsif (defined $to) {
		for my $eid(sort keys %{$to->edges} ) {
      my $edge=$self->get_edge_by_id($eid);
			next unless $edge->to eq $to->id ;
			next if defined ($filter->{id}) && $filter->{id} ne $edge->id;
			if (!defined $filter->{data}) {
				push @$found, $edge;
				next;
			}
			my$t;
			next if defined ($t=$filter->{data}{edge_type}) && $t  ne $edge->data->{edge_type};
			push @$found, $edge;
      last if @$found >=$limit;
		}
	}

  return $found;

  #my $to = $self->{nodes}[$i_from];
  #my $to= $self->{nodes}[$i_to];
  #return () if ref $from ne 'ARRAY' || ref $to ne 'ARRAY';
  #my @result;

}



sub add_edge {
	my ($self, $edge, $partition)=@_;
	my $edges = $self->edges;
	my $nodes= $self->nodes;
  my $id = $edge->id;
  if (defined $id) {
    if (defined  $self->get_edge_by_id($id)) {
      return ()
    }
  }
  else {
    die "edge id not defined";
    #$id =$self->next_id;
    $edge->id($id);
  }
	$edges->{$edge->id}=$edge;
	my $from  = $self->get_node_by_id($edge->from);
	my $to  = $self->get_node_by_id($edge->to);
  if (!defined $from) {
    die sprintf "no from id: from %s, to %s, edge %s", $edge->from, $edge->to, $edge->id;
  }
	if (!defined $to) {
    die sprintf "no to id: from %s, to %s, edge %s", $edge->from, $edge->to, $edge->id;
  }
	my $edges_from = $from->edges;
	my $edges_to = $to->edges;
	$edges_from->{$edge->id}=$to->id;
	$edges_to->{$edge->id}=$from->id;
	$edge->{partition}= $partition if $partition;
  $self->assign_elt_partition($from,$edge->{partition});
  $self->assign_elt_partition($to,$edge->{partition});
  $self->{changed}++;
	return $edge->id
}

sub delete_edge {
  my ($self,$eid)=@_;
  my $edges =$self->edges;
  my $nodes = $self->nodes;
  my ($from, $to, $edge);
  if (!defined ($edge = $self->get_edge_by_id($eid))) {
    die "edge id $eid not found"
  }

  if (!defined ( $from = $self->get_node_by_id($edge->from))) {
    warn sprintf "edge %s: from node %s does not exist", $eid, $edge->from
		;
  }
  else {
    delete $from->edges->{$eid};
  }
  if (!defined ( $to = $self->get_node_by_id($edge->to))) {
    warn sprintf "edge %s: to node %s does not exist", $eid, $edge->to

  }
  else {
    delete $to->edges->{$eid};
  }



  delete $edges->{$eid};
  1;

}


sub add_peers {
	my ($self, $edge_type,$from,$to)=@_;
	die "add_peers: peers must be array refs"
    if ref $from ne 'ARRAY' || ref $to ne 'ARRAY';
	for my $f (@$from) {
		for my $t (@$to) {
			if (!$self->{nodes}->[$f] || !$self->{nodes}->[$t]) {
				die "add_peers: wrong peers";
			}
			push @{$self->{links}}, [$edge_type, $f, $t];
      $self->{changed}++;
			push @{$self->{nodes}->[$f][2]}, @{$self->{links}}-1;
			push @{$self->{nodes}->[$t][1]}, @{$self->{links}}-1;
		}
	}
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

sub inverse {
  my ($self,$elements,$el_set)=@_;
  $el_set//={};
  my $inv_set={};
  for (my $i=0;$i<@$elements;$i++) {
    if (!defined $el_set->{$i}) {
      $inv_set->{$i}=1;
    }
  }
  return $inv_set;
}

sub graph_split {
  my ($self)=@_;
  my $nodes= $self->nodes;
  my $edges= $self->edges;
  my $ndetach={};
  my $edetach={};

  #my ($stats,$scratch) = $self->graph_stats();

  my $seen={};
  #my $collapsed=$self->get_collapsed_roots();

  for my $eid (keys %$edges) {
    my $edge=$self->get_edge_by_id($eid);
    #my $ed=$self->get_edge_data($i);
    my $rootfrom = $self->get_node_by_id($edge->from);
    my $rootto = $self->get_node_by_id($edge->to);
    #my $rootto= get_root($self,$er->[2]);

    if ($edge->data->{edge_type} eq 'referer') {
      $ndetach->{$edge->from}++;
      $ndetach->{$edge->to}++;
      $edetach->{$eid}++;
    }
  }

  #for my $root (sort keys %$collapsed) {
  #  my $rootc = $collapsed->{$root};
  #  if (!@{$rootc->[1]} && !@{$rootc->[2]}) {
  #    unless ($rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}}) {
  #      $ndetach->{$root}++;
  #    }
  #  }
  #}

  return ($ndetach, $edetach);
}

sub old_graph_split {
  my ($self)=@_;
  my $nodes= $self->{nodes};
  my $edges= $self->{links};
  my $ndetach={};
  my $edetach={};

  my ($stats,$scratch) = $self->graph_stats();

  my $seen={};
  my $collapsed=$self->get_collapsed_roots();

  for (my $i=0;$i<@$edges;$i++) {
    my $er=$self->get_edge($i);
    my $ed=$self->get_edge_data($i);
    my $rootfrom = get_root($self,$er->[1]);
    my $rootto= get_root($self,$er->[2]);

    if ($ed->{edge_type} eq 'referer') {
      $ndetach->{$rootfrom}++;
      $ndetach->{$rootto}++;
      $edetach->{$i}++;
    }
  }

  for my $root (sort keys %$collapsed) {
    my $rootc = $collapsed->{$root};
    if (!@{$rootc->[1]} && !@{$rootc->[2]}) {
      unless ($rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}}) {
        $ndetach->{$root}++;
      }
    }
  }

  return ($ndetach, $edetach);
}

sub graph_split2 {
  my ($self)=@_;
  my $nodes= $self->{nodes};
  my $edges= $self->{links};
  my $ndel={};
  my $nkeep={};
  my $edel={};
  my $ekeep={};

  #$self->defragment();
  #consistency_check($self);

  my ($stats,$scratch) = $self->graph_stats();
  #printf STDERR "stats before deletion: %s\n", dumper($stats);
  #my ($collapsed,$seen)=({},{});
=pod
my $flt_root = sub {
    my ($root)=@_;
    my $rootc = $collapsed->{$root} || get_collapsed_node($self, $root, $seen);
      if (!$collapsed->{$root} ++ ) {
        my $rootc = get_collapsed_node($self, $root,$seen);
        if ($rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}} ){
          $nkeep->{$root}++;
        }
  };
=cut
  my $seen={};
  my $collapsed=$self->get_collapsed_roots();
  for (my $i=0;$i<@$edges;$i++) {
    my $er=$self->get_edge($i);
    my $ed=$self->get_edge_data($i);
    my $rootfrom = get_root($self,$er->[1]);
    my $rootto= get_root($self,$er->[2]);

    if ($ed->{edge_type} eq 'referer') {
      my $collfrom= $collapsed->{$rootfrom};
      my $collto = $collapsed->{$rootto};
      my $ok =1;
      if ($collfrom && $collfrom->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$collfrom->[0]{node_type}} ) {
        $ok=0;
      }
      if ($collto && $collto->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$collto->[0]{node_type}} ) {
        $ok=0;
      }
      if ($ok) {
        $ndel->{$rootfrom}{$i}++;
        $ndel->{$rootto}{$i}++;
        $edel->{$i}++;
      }
      else {
        $nkeep->{$rootfrom}++;
        $nkeep->{$rootto}++;
      }
    }
    else {
      $nkeep->{$rootfrom}++;
      $nkeep->{$rootto}++;
    }
  }

  #for my $node(keys %$nkeep) {
  #  if ($ndel->{$node}) {
  #    delete $ndel->{$node};
  #  }
  #}

=pod
  my $seen={};
  for my $root(keys %$ndel) {
    next if $nkeep->{$root};
    if (!$seen->{$root} ++ ) {
      my $rootc = get_collapsed_node($self, $root,$seen);
      if ($rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}} ){
        $nkeep->{$root}++;
      }
      #else {
      #  $ndel->{$root}++;
      #}
    }
  }

=cut

  for my $root (sort keys %$collapsed) {
    my $rootc = $collapsed->{$root};
    if (!@{$rootc->[1]} && !@{$rootc->[2]}) {
      if ($rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}}) {
        $nkeep->{$root}++;
      }
      else {
        $ndel->{$root}//={};
      }
    }
    else {
      if (!$ndel->{$root} && !$nkeep->{$root}) {
        die "root node $root edges were missed somehow";
      }
    }
  }

  my $detach_roots = {};
  my $detach_edges = {};

  #my $flt = sub {
  #  my ($n,$nr,$level)=@_;
  #  $detach_nodes->{$n}++;
  #  1;
  #};

  for my $root(keys %$ndel) {
    if (!$nkeep->{$root}) {
      $detach_roots->{$root}++;
    }
    else {
      for my $edge(keys %{$ndel->{$root}}) {
        $ekeep->{$edge}++;
      }
    }
  }

  for my $edge(keys %$edel) {
    if (1 || !$ekeep->{$edge}) {
      $detach_edges->{$edge}++
    }
  }

  return ($detach_roots, $detach_edges);
}

sub intersect {
  my ($self,$set1,$set2)=@_;
  my $rset={};
  while(my ($key,$value)=each(%$set1)) {
    if (exists $set2->{$key}) {
      $rset->{$key}=$value;
    }
  }
  return $rset;
}

sub minus {
  my ($self,$set1,$set2)=@_;
  my $rset={};
  while(my ($key,$value)=each(%$set1)) {
    if (! exists $set2->{$key}) {
      $rset->{$key}=$value;
    }
  }
  return $rset;
}


sub pivot {
  my ($self,$node_set,$edge_set)=@_;
  my $nodes = $self->nodes;
  my $edges = $self->edges;
  my $new_edge_set={};
  my $new_node_set={};
  my $node_intersect={};
  my $edge_intersect={};

  #my $seen_root={};
  #if (!$roots || !$node2root) {
  #  ($roots,$node2root)=$self->get_roots();
  #}

  #my $flt = sub {
  #  my ($n,$nr,$level)=@_;
  #  $new_node_set->{$n}++;
  #  0;
  #};

  for my $eid (keys %$edges) {
    my $edge = $self->get_edge_by_id($eid);
    next if $edge_set->{$eid};
    $new_edge_set->{$eid}++;
    my ($from) = $edge->from;
    my ($to) = $edge->to;

    if (!defined $from) {
      die sprintf "edge %s: node %s not found", $eid,$edge->from
    }
    if (!defined $to) {
      die sprintf "edge %s: node %s not found", $eid,$edge->to
    }
    $new_node_set->{$from}++;
    $new_node_set->{$to}++;
  }

  for my $nid (keys %$nodes) {
    next if $node_set->{$nid};
    $new_node_set->{$nid}++;
  }

  return ($new_node_set, $new_edge_set);
}

sub old_pivot {
  my ($self,$root_set,$edge_set, $roots,$node2root)=@_;
  my $nodes = $self->{nodes};
  my $edges = $self->{links};
  my $new_edge_set={};
  my $new_node_set={};
  my $seen_root={};
  if (!$roots || !$node2root) {
    ($roots,$node2root)=$self->get_roots();
  }

  my $flt = sub {
    my ($n,$nr,$level)=@_;
    $new_node_set->{$n}++;
    0;
  };

  for (my $i=0;$i<@$edges;$i++ ) {
    next if !defined (my $e = $edges->[$i]);
    next if $edge_set->{$i};
    $new_edge_set->{$i}++;
    my ($from,$to)=($e->[1],$e->[2]);
    my $rootfrom = $node2root->{$from} or
      die "node $from has no root";
    my $rootto = $node2root->{$to} or
      die "node $to has no root";
    if (!$new_node_set->{$rootfrom}++) {
      $self->descendants_depth_last($rootfrom,$flt);
    }
    if (!$new_node_set->{$rootto}++) {
      $self->descendants_depth_last($rootto,$flt);
    }
  }

  for (my $i=0;$i<@$nodes;$i++ ) {
    next if !defined (my $nr = $nodes->[$i]);
    my $root = $node2root->{$i};
    die "wrong node2root map: $i" if !defined $root;
    next if $root_set->{$root};
    if (!$new_node_set->{$root}++) {
      $self->descendants_depth_last($root,$flt);
    }
  }

  return ($new_node_set, $new_edge_set);
}

=pod
sub delete_junk {
  my ($self)=@_;
  my $nodes= $self->{nodes};
  my $edges= $self->{links};
  #my $ndel={};
  #my $nkeep={};
  #my $edel={};

  $self->defragment();
  consistency_check($self);

  my ($stats,$scratch) = $self->graph_stats();
  printf STDERR "stats before deletion: %s\n", dumper($stats);

  my ($ndel, $edel) = graph_split($self);
  for my $edge (keys %$edel) {
    delete $edges->[$edge];
  }

  for my $node (keys %$ndel) {
    $self->delete_node_incst_recursive($node);
  }

  #for my $node (keys %{$scratch->{isolated_root}}) {
  #  next if !defined $nodes->[$node];
  #  my $rootc=$scratch->{isolated_root}{$node};
  #  next if $rootc->[0]{node_type} && grep { /^(query|topic|session)$/ } @{$rootc->[0]{node_type}};
  #  $self->delete_node_incst_recursive($node);
  #}

  my ($stats,$scratch) = $self->graph_stats();
  printf STDERR "stats after deletion: %s\n", dumper($stats);
  $self->rebuild_back_links();

  my ($stats,$scratch) = $self->graph_stats();
  printf STDERR "stats after rebuild: %s\n", dumper($stats);


  consistency_check($self);
  #$self->defragment_links();
  #consistency_check($self);
  $self->defragment();

  my ($stats,$scratch) = $self->graph_stats();
  printf STDERR "stats after defragmentation: %s\n", dumper($stats);

  #consistency_check($self);
  consistency_check($self);
  print STDERR "deletion went OK\n";
}


=cut

sub db_subset {
  my ($self, $node_set, $edge_set)=@_;
  my $flt = sub {
    my ($ctx,$keys)=@_;
    my $ix={};
    if (ref $keys eq 'HASH' && $#$ctx==3 && $ctx->[1] eq 'nodes' && ref $ctx->[2] eq 'ARRAY' ) {
      return $node_set;
    }
    if (ref $keys eq 'HASH' && $#$ctx==3 && $ctx->[1] eq 'links' && ref $ctx->[2] eq 'ARRAY' ) {
      return $edge_set;
    }
    return 1;
  };

=pod
  my $qg_set=common::clone(
                           {request_files=>$self->{request_files}
                            , nodes =>$self->{nodes}
                            , links =>$self->{links}
                            }, $flt
                           );
=cut

  my $qg_set=bless (Clone::clone({request_files=>$self->{request_files}
                            , nodes =>{}
                            , edges =>{}
                            }), 'graph_object');
  for my $nid(keys %$node_set)  {
    my $node;
    if (defined ($node=$self->get_node_by_id($nid))) {
      $node = bless(Clone::clone($node), 'graph_object::node');
      $node->edges({});
      eval {
        $qg_set->add_node($node);
      };
      if ($@) {
        die $@;
      }
    }
  }

  for my $eid(keys %$edge_set)  {
    my $edge;
    if (defined ($edge=$self->get_edge_by_id($eid))) {
      my @partitions = $edge->partitions;
      if (@partitions!=1) {
        my $partition = (pop @partitions) // $self->current;
        $self->assign_elt_partition($edge,$partition,1)
      }
      $qg_set->add_edge(bless (Clone::clone($edge), 'graph_object::edge'));
    }
  }
  return $qg_set;
}

sub db_split {
  my ($self,$nodes_detach,$edges_detach)=@_;
  #my ($nodes_detach) = $self->get_descendants_depth_last($roots_detach);
  my $detached = $self->db_subset($nodes_detach,$edges_detach);

  my ($nodes_left,$edges_left) = $self->pivot($nodes_detach, $edges_detach);
  #my ($nodes_left) = $self->get_descendants_depth_last($roots_left);
  my $left = $self->db_subset($nodes_left, $edges_left);
  return ($detached,$left);
}

=pod
sub db_split {
  my ($self,$roots_detach,$edges_detach)=@_;
  my ($nodes_detach) = $self->get_descendants_depth_last($roots_detach);
  my $detached = $self->db_subset($nodes_detach,$edges_detach);

  my ($roots_left,$edges_left) = $self->pivot($roots_detach, $edges_detach);
  my ($nodes_left) = $self->get_descendants_depth_last($roots_left);
  my $left = $self->db_subset($nodes_left, $edges_left);
  return ($detached,$left);
}
=cut


sub rebase {
  my ($self,$min_node, $min_edge)=@_;
  die "wrong min node,edge" unless $min_node >=0 && $min_edge >=0;
  #return if $min_node==0 && $min_edge==0;
  #die "cannot rebase to $min_node" if ($min_node < @{$self->{nodes}});
  #die "cannot rebase to $min_edge" if ($min_edge < @{$self->{links}});
  if ($min_node > 0) {
    for (my $i=@{$self->{nodes}}-1;$i>=0;$i--) {
      $self->move_node($i,$min_node+$i);
    }
  }

  if ($min_edge> 0) {
    for (my $i=@{$self->{links}}-1;$i>=0;$i--) {
      $self->{links}[$i+$min_edge] = $self->{links}[$i];
      $self->{links}[$i]= undef;
    }
  }

  $self->rebuild_back_links();
}

sub compare_node{
  my ($self,$sn,$att,$an, $exclude)=@_;
  $exclude//={ edges=>1, storage=>1, partition=>1 }; # FIX: remove storage
  #my ($snd) = $sn->data;
  #my ($and) = $an->data;
  my ($snm) = $sn->meta;
  my ($anm) = $an->meta;
  if (!defined $snm && !defined $anm)  {
    return ();
  }
  #$snd //={};
  #$and//={};
  #my $keys={};
  my $diff = {};
  my $diffs;
  #my $keys=[qw(id key data parent children cid)];
  for my $key ((keys %$snm),(keys %$anm)) {
  #for my $key (@$keys) {
    #next if $key eq 'id' || $key eq 'prev_id';
    next if $exclude->{$key} ;
    $diff->{$key}||= (!Compare($snm->{$key}, $anm->{$key}) && ($diffs=1));
=pod
    if (ref $snd->{$key} ne '' && ref $and->{$key} ne '') {
      $diff->{$key}||= (!Compare($snd->{$key}, $and->{$key}) && ($diffs=1));
    }
    else {
      $diff->{$key}||= (!Compare($snd->{$key}, $and->{$key}) && ($diffs=1));
      $diff->{$key}||=((defined ($snd->{$key}) <=> defined ($and->{$key}))
                        || ($snd->{$key} cmp $and->{$key})
                        )&&($diffs=1);
    }
=cut
  }
  return $diff if $diffs;
  return ();
}


sub assign_elt_partition {
	my ($self,$elt,$partitions,$clear)=@_;
	#$node=$self->get_node($node);

	my $current= $elt->{partition};
	return () if ($current eq $partitions);
	my @partition=$clear || $current eq '' ?  (): split m{/},$current;
	if ($partitions ne '') {
		for my $partition(split m{/}, $partitions) {
			unless (grep { $_ eq $partition } @partition) {
				push @partition, $partition;
			}
		}
	}
	$elt->{partition}=join '/', @partition;
  return @partition;
=pod
	my %storage=$clear || $storage eq '' ?  (): map {$_,1} split m{/},$storage;
	if ($partition ne '') {
		$storage{$_}=1 for split m{/}, $partition;
	}
	$node->{storage}=join ',', keys %storage;
=cut
}

sub assign_partition {
	my ($self,$partitions,$clear,$nodes,$edges)=@_;
  $nodes//=$self->nodes;
  $edges//=$self->edges;
  my $changed={};
	for my $nid(%$nodes) {
		my $node = $self->get_node_by_id($nid);
    next if !$node;
		for my $c($self->assign_elt_partition($node,$partitions,$clear)) {
      $changed->{$c}++;
    }

	}
	for my $eid(%$edges) {
		my $edge = $self->get_edge_by_id($eid);
    next if !$edge;
		for my $c ( $self->assign_elt_partition($edge,$partitions,$clear)){
      $changed->{$c}++;
    }
	}
  return $changed;
}

sub is_contained {
  my ($self, $na,$nb)=@_;
  for my $key (keys %$na) {
    if (ref $na->{$key} eq '') {
      next;
    }

  }
}

sub merge_graphs {

  my ($self,$att)=@_;
  my $nodes=$self->nodes;
  my $edges=$self->edges;

  my $stats={qw(nodes_added 0 nodes_merged 0 edges_added 0 edges_merged 0)};

  for my $nid (sort keys %{$att->nodes}) {
    my $an = $att->get_node_by_id($nid);
    my $sn = $self->get_node_by_id($nid);
    if (!defined $sn) {
      $sn = $self->get_node_by_key($an->key);
    }
    if (defined $sn ) {
      my $cmp = $self->compare_node($sn,$att,$an);
      if ($cmp) {
        die sprintf "merging nodes %s,%s differ on keys: %s", $sn->id, $an->id, join ',', sort grep { $_ ne '' } keys %$cmp;
      }
      $stats->{nodes_merged}++;
    }
    else {
			(undef,$sn)= $self->add_node($an);
      $stats->{nodes_added}++;

      #$nodes->[$an]=$att->{nodes}[$an];
    }
		$self->assign_elt_partition($sn,$self->{current});
    #descendants_depth_last($att,$root,$merge_root);

  }

  for my $eid (sort keys %{$att->edges}) {
    my $se = $self->get_edge_by_id($eid);
    if (defined $se) {
      die sprintf "edge %s already in target graph",$eid;
    }
    my $ae = $att->get_edge_by_id($eid);

    if (!$self->add_edge($ae)) {
      die sprintf "cannot add edge %s", $eid
    }
		$self->assign_elt_partition($se,$self->{current});

    $stats->{edges_added}++;
  }
  return $stats;
}


=pod

sub old_merge_graphs {

  my ($self,$att)=@_;
  my $nodes=$self->{nodes};
  my $edges=$self->{links};
  my $min_node= $self->{nodes} ? @{$self->{nodes}} : 0;
  my $min_edge= $self->{links} ? @{$self->{links}} : 0;
  $att->rebase($min_node,$min_edge);
  $att->derive_data();
  $att->consistency_check();
  my ($at_roots,$at_node2root)=$att->get_roots();
  my ($roots,$node2root)=$self->get_roots();
  my $merge_root = sub {
    my ($an,$anr,$level)=@_;
    my $key;
    if (!defined ($key= $anr->[0]{key})) {
      die "node $an has no key";
    }
    if (defined (my $sn = $self->get_node($key)) ) {
      my $cmp = $self->compare_node_data($sn,$att,$an);
      if ($cmp) {
        die sprintf "merging nodes $sn, $an differ on keys: %s", join ',', sort keys %$cmp;
      }
      my ($src,$dst) = switch_links_incst($att,$an,$sn);
      print STDERR sprintf "switched links: %s->%s, [%s], [%s]\n",
        $an, $sn, join (',', @$src), join (',', @$dst);
      #die "attachment has the same key: $nr->[0]{key}";
    }
    else {
      $nodes->[$an]=$att->{nodes}[$an];
    }
    0;
  };

  if ($min_node > 0 )  {
    for my $root (sort keys %$at_roots) {
      my $an = $att->{nodes}[$root];
      next unless defined $an;
      descendants_depth_last($att,$root,$merge_root);
    }
    $self->derive_data();
  }

  if ($min_edge > 0 )  {
    for (my $i=$min_edge;$i<@{$att->{links}};$i++) {
      $edges->[$i]=$att->{links}[$i];
    }
    #$self->derive_data();
  }

  $self->rebuild_back_links();
  1;
}

=cut



sub edge_count {
  my ($self)=@_;
  return 0+grep {defined $_} values %{$self->{edges}};
}

sub node_count {
  my ($self)=@_;
  return 0+grep {defined $_} values %{$self->{nodes}};
}

sub derive_edges { # FIX: inefficient
  my ($self, $node)=@_;
  my $id = $node->id;
  my $edges = $self->edges;
  my $node_edges = {};
  while (my ($eid,$edge)=each(%$edges)) {
    if ($edge->{from} eq $id ) {
      $node_edges->{$eid}=$edge->{to};
    }
    if ($edge->{to} eq $id ) {
      $node_edges->{$eid}=$edge->{from};
    }
  }
  return $node_edges;
}

sub derive_edges_for_nodes {
  my ($self, $nodes)=@_;
  return () if ref $nodes ne 'HASH';
  my $node_map={};
  #my $id = $node->id;
  my $edges = $self->edges;
  #my $node_edges = {};
  while (my ($eid,$edge)=each(%$edges)) {
    if ($nodes->{$edge->{from}}) {
      $node_map->{$edge->{from}}{$eid}=$edge->{to};
      #$node_edges->{$eid}=$edge->{to};
    }
    if ($nodes->{$edge->{to}}) {
      $node_map->{$edge->{to}}{$eid}=$edge->{from};
      #$node_edges->{$eid}=$edge->{to};
    }
  }
  for my $nid(keys %$node_map) {
    next unless my $node = $self->get_node_by_id($nid);
    $node->edges($node_map->{$nid});
  }
  return $node_map;
}


1;
}

package graph_object::node;
{
use strict;
=pod
sub get_root {
	my ($self, $node)=@_;
	my $n=$self->get_node($node);
  if (!defined $n) {
    die "node $node not found";
  }
	for (my $i=0;$i<10;$i++) {
		my $nr = $self->{nodes}->[$n];
		if (ref $nr ne 'ARRAY' || ref $nr->[0] ne 'HASH') {
			die "parent $n of node $node is inconsistent";
		}
		if (my $nn = $self->get_node($nr->[0]{parent})) {
			$n=$nn;
		}
		else {
			return $n;
		}
	}
}
=cut


sub TO_JSON { return { %{ shift() } }; }

sub new {
  my ($self, $meta, $key, $id)=@_;
	$meta->{edges}//={};
=pod
  my $meta;
  if (ref $data eq '' ){
    if ($data eq '') {
      die "no data for new node";
    }
    $meta = { ( key => $key//$data, data => {}), $id ? (id=>$id):() };
  }
  elsif ($key eq '') {
    die "key is empty";
  }
  else {
    $meta = { ( key => $key, data => $data, edges=>{}),  $id ? (id=>$id):()    };
  }
=cut
  return bless $meta;
}

sub meta {
  return $_[0];
}

sub set {
  my ($self,$what,$value)=@_;
  return () if (!defined $value);
  if ($what eq 'nodes') {
    $self->nodes($value);
  }
  if ($what eq 'edges') {
    $self->edges($value);
  }
}

sub edges  {
  my ($self,$edges)=@_;
  return $self->{edges}=$edges if (defined $edges );
  return $self->{edges};
}

#sub key  {
#  my ($self,$key)=@_;
#  return $self->{key}=$key if (defined $key );
#  return $self->{key};
#}

sub id  {
  my ($self,$id)=@_;
  return $self->{id}=$id if (defined $id );
  return $self->{id};
}

#partition
sub partition  {
  my ($self,$partition)=@_;
  return $self->{partition}=$partition if (defined $partition );
  return $self->{partition};
}

#sub cid  {
#  my ($self,$cid)=@_;
#  return $self->{cid}=$cid if (defined $cid );
#  return $self->{cid};
#}


sub data  {
  my ($self,$data)=@_;
  return $self->{data}=$data if (defined $data );
  return $self->{data};
}

#storage
#sub storage  {
#  my ($self,$storage)=@_;
#  return $self->{storage}=$storage if (defined $storage );
#  return $self->{storage};
#}

sub g {
  shift->{g};
}

sub descend_dl {
  my ($node, $parent, $root, $ffun, $level, @args)=@_;
  #my $n = old_get_node($self,$node);
  #my $nr = $self->{nodes}[$n];
  #return 0 if (ref $nr ne 'ARRAY');
  return 1 if $ffun->($node->{id}, $node, $parent, $root, $level, @args);
  if ($node->{children}) {
    for my $ch(keys %{$node->{children}}) {
      return 1 if descend_dl($root->{descendants}{$ch}, $node, $root, $ffun, $level +1, @args);
    }
  }

  return 0 ;
}

sub descend_df {
  my ($node, $parent, $root, $ffun, $level, @args)=@_;
  #my $n = old_get_node($self,$node);
  #my $nr = $self->{nodes}[$n];
  #return 0 if (ref $nr ne 'ARRAY');
  if ($node->{children}) {
    for my $ch(keys %{$node->{children}}) {
      return 1 if descend_df($root->{descendants}{$ch}, $node, $root, $ffun, $level +1, @args);
    }
  }
  return 1 if $ffun->($node->{id}, $node, $parent, $root, $level, @args);
  return 0 ;
}


#my $child_id = $to ->add_child({source=>'fiddler', ts=>common::parsetime($ts)});


#my $ch = $seen_nodes->{$n} = { cid=>$n, parent=>$pd, data=>$data};

sub find_children {
  my ($self,$data)=@_;
  $data //={};
  my $start_ts=$data->{start_ts};
  my $end_ts=$data->{end_ts};

  my @match;
  my $flt = sub {
    my ($nid, $node, $parent, $root, $level, @args)=@_;
    my $ndata =$node->{data};
    my $matched;
    for my $key (keys %$data) {
      next if $key eq 'start_ts' || $key eq 'end_ts';
      if (exists $ndata->{$key} && $ndata->{$key} eq $data->{$key}) {
        $matched=1;
      }
      else {
        $matched=0;
        last;
      }
    }
    if (!defined $matched) {
      my $ts =$ndata->{ts};
      if (defined $ts) {
        if ((!defined $start_ts || $ts >= $start_ts) && (!defined $end_ts || $ts < $end_ts)) {
          $matched=1;
        }
        else {
          $matched=0;
        }
      }
    }
    if (defined $matched) {
      push @match, $nid if $matched;
    }
    else {
      push @match, $nid if 0==keys %$data
    }
    0;
  };
  #graph_object::node::descend_dl($node, undef, $node, $proc,0);
  descend_dl($self, undef, $self, $flt, 0);
  return  @match ? \@match : ()

}

sub add_child {
  my ($self,$data, $pid, $id)=@_;
  if (!defined $id) {
    die sprintf "id missing";
  }
  #$id //= $self->g->next_id;
  my $parent;
  if ($pid eq $self->id  ) { #root
    $parent=$self;
  }
  else {
    $parent = $self->{descendants}{$pid}
  }
  if (!defined $parent) {
    die sprintf "parent does not exist: id=%s, parent=%s, root=%s", $id,$pid,$self->id;
  }

  if (defined $parent->{children}{$id}) {
    die sprintf "child already exists: id=%s, parent=%s, root=%s", $id,$pid,$self->id;
  }

  my $ch = $self->{descendants}{$id}={ id=>$id, parent=>$pid, root=>$self->id,  data=>$data};
  $parent->{children}{$id}=1;
  return wantarray ? ($id,$ch) : $id;
}

sub get_child {
  my ($self, $id)=@_;
  if ($id eq $self->id) {
    return $self;
  }
  $self->{descendants}{$id};
}

sub key {
  my ($self,$key)=@_;
  if (defined $key) {
    return $self->data->{fixed_key}=$key;
  }
  if (defined ($key = $self->{data}{fixed_key})) {
    return wantarray ? ($key, 'fixed') : $key;
  }
  if (defined ($key = $self->{data}{key}//$self->{key})) {
    return wantarray ? ($key, 'old') : $key;
  }
  $key = $self->derive_key();
  return wantarray ? ($key, 'derived') : $key;
}

# $self can be anything if $data specified

sub derive_data {
  my ($data, $key)=@_;
  if (defined $key) {
    my ($url,$query,$topic);
    my $node_type= $data->{node_type};
    if ($node_type eq 'url' && !defined $data->{url}) {
      ($data->{url})=$key =~ m{^url:(.*)};
    }
    elsif ($node_type eq 'query' && !defined $data->{query}) {
      ($data->{query})=$key =~ m{^google:(.*)};
    }
    elsif ($node_type eq 'topic' && !defined $data->{topic}) {
      ($data->{topic})=$key =~ m{^topic:(.*)};
    }
  }
}



sub derive_key {
  my ($self, $data)=@_;
  $data//=$self->{data};
  if (ref $data ne 'HASH' ){
    die "wrong arguments to derive_key";
  }

  my ($url, $key);
  if (!defined ($key=$data->{fixed_key})) {
    if ($data->{node_type} eq 'url') {
      die (sprintf "data has no url to derive key:\n%s", graph_object::dumper($data))
        if !defined ($url=$data->{url});
      $key = "url:$url";
    }
    elsif ($data->{node_type} eq 'query') {
      if ((!defined $data->{url} || $data->{url}=~/google\.(com|ru)/) && defined $data->{query}) {
        $key = "google:$data->{query}";
      }
      if (!defined $key) {
        die (sprintf "cannot derive a key for query node:\n%s", graph_object::dumper($data))
      }
    }
    elsif ($data->{node_type} eq 'topic' ) {
      if ($data->{topic} ne '') {
        $key = "topic:$data->{topic}";
      }
      if (!defined $key) {
        die (sprintf "cannot derive a key for topic node:\n%s", graph_object::dumper($data))
      }
    }
    elsif ($data->{node_type} eq 'request' && ref  $self eq __PACKAGE__) {
      #my $req_node=$self->get_request()
      $key = "req:".$self->id;
    }

  }
  if (!defined $key) {
    die (sprintf "cannot derive a key for data %s", graph_object::dumper($data));
  }
  return $key;
}

sub partitions  {
  my ($self)=@_;
  return $self->{partition} ne '' ? (split m{/},$self->{partition}) : ();
}



1;
}


package graph_object::edge;
{
use strict;
sub TO_JSON { return { %{ shift() } }; }
sub new2 {
  my ($self,$data,$from,$to,$id, $from_child, $to_child, $partition)=@_;
  my $meta = { data=> $data, from=>$from , to=>$to
              , defined $id ? (id=>$id):()
              , $from_child ? (from_child=>$from_child):()
              , $to_child ? (to_child=>$to_child):()
							, $partition ? (partition=>$partition) :()
             };
  return bless $meta;
}

sub new {
  my ($self,$meta)=@_;
  return bless $meta;
}


sub id  {
  my ($self,$id)=@_;
  return $self->{id}=$id if (defined $id );
  return $self->{id};
}

sub data  {
  my ($self,$data)=@_;
  return $self->{data}=$data if (defined $data );
  return $self->{data};
}


sub from {
  my ($self,$from)=@_;
  return $self->{from}=$from if (defined $from );
  return $self->{from};
}

sub to {
  my ($self,$to)=@_;
  return $self->{to}=$to if (defined $to );
  return $self->{to};
}

sub child_from {
  my ($self,$child_from)=@_;
  return $self->{child_from}=$child_from if (defined $child_from );
  return $self->{child_from};
}

sub child_to {
  my ($self,$child_to)=@_;
  return $self->{child_to}=$child_to if (defined $child_to );
  return $self->{child_to};
}

#partition
sub partition  {
  my ($self,$partition)=@_;
  return $self->{partition}=$partition if (defined $partition );
  return $self->{partition};
}

sub partitions  {
  my ($self)=@_;
  return $self->{partition} ne '' ? (split m{/},$self->{partition}) : ();
}




1;
}
