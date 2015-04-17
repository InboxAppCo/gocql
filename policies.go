// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//This file will be the future home for more policies
package gocql

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

//RetryableQuery is an interface that represents a query or batch statement that
//exposes the correct functions for the retry policy logic to evaluate correctly.
type RetryableQuery interface {
	Attempts() int
	GetConsistency() Consistency
}

// RetryPolicy interace is used by gocql to determine if a query can be attempted
// again after a retryable error has been received. The interface allows gocql
// users to implement their own logic to determine if a query can be attempted
// again.
//
// See SimpleRetryPolicy as an example of implementing and using a RetryPolicy
// interface.
type RetryPolicy interface {
	Attempt(RetryableQuery) bool
}

/*
SimpleRetryPolicy has simple logic for attempting a query a fixed number of times.

See below for examples of usage:

	//Assign to the cluster
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

	//Assign to a query
 	query.RetryPolicy(&gocql.SimpleRetryPolicy{NumRetries: 1})
*/
type SimpleRetryPolicy struct {
	NumRetries int //Number of times to retry a query
}

// Attempt tells gocql to attempt the query again based on query.Attempts being less
// than the NumRetries defined in the policy.
func (s *SimpleRetryPolicy) Attempt(q RetryableQuery) bool {
	return q.Attempts() <= s.NumRetries
}

//HostSelectionPolicy is an interface for selecting
//the most appropriate host to execute a given query.
type HostSelectionPolicy interface {
	SetHosts
	SetPartitioner
	//Pick returns an iteration function over selected hosts
	Pick(*Query) NextHost
}

//NextHost is an iteration function over picked hosts
type NextHost func() *HostInfo

//NewRoundRobinHostPolicy is a round-robin load balancing policy
func NewRoundRobinHostPolicy() HostSelectionPolicy {
	return &roundRobinHostPolicy{hosts: []HostInfo{}}
}

type roundRobinHostPolicy struct {
	hosts []HostInfo
	pos   uint32
	mu    sync.RWMutex
}

func (r *roundRobinHostPolicy) SetHosts(hosts []HostInfo) {
	r.mu.Lock()
	r.hosts = hosts
	r.mu.Unlock()
}

func (r *roundRobinHostPolicy) SetPartitioner(partitioner string) {
	// noop
}

func (r *roundRobinHostPolicy) Pick(qry *Query) NextHost {
	// i is used to limit the number of attempts to find a host
	// to the number of hosts known to this policy
	var i uint32 = 0
	return func() *HostInfo {
		if len(r.hosts) == 0 {
			return nil
		}

		var host *HostInfo
		r.mu.RLock()
		// always increment pos to evenly distribute traffic in case of
		// failures
		pos := atomic.AddUint32(&r.pos, 1)
		if int(i) < len(r.hosts) {
			host = &r.hosts[(pos)%uint32(len(r.hosts))]
			i++
		}
		r.mu.RUnlock()
		return host
	}
}

//NewTokenAwareHostPolicy is a token aware host selection policy
func NewTokenAwareHostPolicy(fallback HostSelectionPolicy) HostSelectionPolicy {
	return &tokenAwareHostPolicy{fallback: fallback, hosts: []HostInfo{}}
}

type tokenAwareHostPolicy struct {
	mu          sync.RWMutex
	hosts       []HostInfo
	partitioner string
	tokenRing   *tokenRing
	fallback    HostSelectionPolicy
}

func (t *tokenAwareHostPolicy) SetHosts(hosts []HostInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// always update the fallback
	t.fallback.SetHosts(hosts)
	t.hosts = hosts

	t.resetTokenRing()
}

func (t *tokenAwareHostPolicy) SetPartitioner(partitioner string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.partitioner != partitioner {
		t.fallback.SetPartitioner(partitioner)
		t.partitioner = partitioner

		t.resetTokenRing()
	}
}

func (t *tokenAwareHostPolicy) resetTokenRing() {
	if t.partitioner == "" {
		// partitioner not yet set
		return
	}

	// create a new token ring
	tokenRing, err := newTokenRing(t.partitioner, t.hosts)
	if err != nil {
		log.Printf("Unable to update the token ring due to error: %s", err)
		return
	}

	// replace the token ring
	t.tokenRing = tokenRing
}

func (t *tokenAwareHostPolicy) Pick(qry *Query) NextHost {
	if qry == nil {
		return t.fallback.Pick(qry)
	}

	routingKey, err := qry.GetRoutingKey()
	if err != nil {
		return t.fallback.Pick(qry)
	}
	if routingKey == nil {
		return t.fallback.Pick(qry)
	}

	var host *HostInfo

	t.mu.RLock()
	// TODO retrieve a list of hosts based on the replication strategy
	host = t.tokenRing.GetHostForPartitionKey(routingKey)
	t.mu.RUnlock()

	if host == nil {
		return t.fallback.Pick(qry)
	}

	// scope these variables for the same lifetime as the iterator function
	var (
		hostReturned bool
		fallbackIter NextHost
	)
	return func() *HostInfo {
		if !hostReturned {
			hostReturned = true
			return host
		}

		// fallback
		if fallbackIter == nil {
			fallbackIter = t.fallback.Pick(qry)
		}

		fallbackHost := fallbackIter()

		// filter the token aware selected hosts from the fallback hosts
		if fallbackHost == host {
			fallbackHost = fallbackIter()
		}

		return fallbackHost
	}
}

//ConnSelectionPolicy is an interface for selecting an
//appropriate connection for executing a query
type ConnSelectionPolicy interface {
	SetConns(conns []*Conn)
	Pick(*Query) *Conn
}

type roundRobinConnPolicy struct {
	conns []*Conn
	pos   uint32
	mu    sync.RWMutex
}

func NewRoundRobinConnPolicy() ConnSelectionPolicy {
	return &roundRobinConnPolicy{}
}

func (r *roundRobinConnPolicy) SetConns(conns []*Conn) {
	r.mu.Lock()
	r.conns = conns
	r.mu.Unlock()
}

func (r *roundRobinConnPolicy) Pick(qry *Query) *Conn {
	pos := atomic.AddUint32(&r.pos, 1)
	var conn *Conn
	r.mu.RLock()
	if len(r.conns) > 0 {
		conn = r.conns[pos%uint32(len(r.conns))]
	}
	r.mu.RUnlock()
	return conn
}

type ReplicationPolicy interface {
	GetReplicas(
		tokens []token,
		hosts []*HostInfo,
	) (replicas [][]*HostInfo)
}

func newReplicationPolicy(
	strategyClass string,
	options map[string]string,
) (ReplicationPolicy, error) {
	if strings.HasSuffix(strategyClass, "SimpleStrategy") {
		return newSimpleReplicationPolicy(options)
	}
	if strings.HasSuffix(strategyClass, "NetworkTopologyStrategy") {
		return newNetworkTopologyReplicationPolicy(options)
	}
	return nil, fmt.Errorf(
		"Unsupported replication strategy '%s'",
		strategyClass,
	)
}

type simpleReplicationPolicy struct {
	replicationFactor int
}

func newSimpleReplicationPolicy(
	options map[string]string,
) (ReplicationPolicy, error) {
	// get the replication factor
	replicationFactorStr, found := options["replication_factor"]
	if !found {
		return nil, errors.New(
			"replication_factor option must be supplied for SimpleStrategy " +
				"replication strategy",
		)
	}

	// parse the replication factor
	replicationFactor, err := strconv.ParseInt(replicationFactorStr, 10, 32)
	if err != nil {
		return nil, fmt.Errorf(
			"replication_factor setting '%s' could not be parsed due to an error: %v",
			replicationFactorStr,
			err,
		)
	}

	return &simpleReplicationPolicy{int(replicationFactor)}, nil
}

func (s *simpleReplicationPolicy) GetReplicas(
	tokens []token,
	hosts []*HostInfo,
) (replicas [][]*HostInfo) {
	ringSize := len(tokens)
	replicationFactor := s.replicationFactor
	if replicationFactor > ringSize {
		// replication factor cannot be greater than the ring size
		replicationFactor = ringSize
	}
	replicas = make([][]*HostInfo, ringSize)

	// find the replicas for every token
	for i := 0; i < ringSize; i++ {
		replicas[i] = make([]*HostInfo, replicationFactor)
		// the first replica is the token owner
		replicas[i][0] = hosts[i]

		// the other replicas are successive hosts around the ring.
	find_replicas:
		for j := 1; j < replicationFactor; j++ {
			candidate := hosts[(i+j)%ringSize]

			// hosts can own successive tokens so check against the existing
			// replicas found
			for k := 0; k < j; k++ {
				if replicas[i][k] == candidate {
					// not unique
					continue find_replicas
				}
			}

			// this candidate is a replica
			replicas[i][j] = candidate
		}
	}

	return replicas
}

type networkTopologyReplicationStrategy struct {
	replicationFactors map[string]int
}

func newNetworkTopologyReplicationPolicy(
	options map[string]string,
) (ReplicationPolicy, error) {
	// each option is a replication factor
	replicationFactors := map[string]int{}
	for optionName, optionValue := range options {
		replicationFactor, err := strconv.ParseInt(optionValue, 10, 32)
		if err != nil {
			return nil, fmt.Errorf(
				"replication factor setting '%s': '%s' could not be parsed due to an error: %v",
				optionName,
				optionValue,
				err,
			)
		}

		replicationFactors[optionName] = int(replicationFactor)
	}

	return &networkTopologyReplicationStrategy{replicationFactors}, nil
}

type dcReplicaVisitInfo struct {
	replicationFactor int
	racks             []string
	rackCount         int

	replicaCount     int
	visitedRackCount int
	visitedRacks     []string

	skipped []*HostInfo
}

func (d *dcReplicaVisitInfo) done() bool {
	return d.replicaCount >= d.replicationFactor
}

func (d *dcReplicaVisitInfo) reset() {
	d.replicaCount = 0
	d.visitedRacks = d.visitedRacks[:0]
	d.visitedRackCount = 0
}

func hostSetAppend(
	set []*HostInfo,
	host *HostInfo,
) (resultSet []*HostInfo, added bool) {
	for i := range set {
		if host == set[i] {
			return set, false
		}
	}

	return append(set, host), true
}

func (n *networkTopologyReplicationStrategy) GetReplicas(
	tokens []token,
	hosts []*HostInfo,
) (replicas [][]*HostInfo) {

	ringSize := len(tokens)
	replicas = make([][]*HostInfo, ringSize)

	// prepare the dc information for these hosts
	dcInfos := map[string]*dcReplicaVisitInfo{}
create_dc_info:
	for _, host := range hosts {
		dc := host.DataCenter
		if dc == "" {
			continue
		}

		replicationFactor, found := n.replicationFactors[dc]
		if !found {
			// not included in strategy
			continue
		}

		dcInfo, found := dcInfos[dc]
		if !found {
			dcInfo = &dcReplicaVisitInfo{
				replicationFactor: replicationFactor,
				racks:             make([]string, 0, ringSize),
				skipped:           make([]*HostInfo, 0, ringSize),
			}
			dcInfos[dc] = dcInfo
		}

		rack := host.Rack
		if rack == "" {
			continue
		}

		// check if the rack is already known
		for i := range dcInfo.racks {
			if rack == dcInfo.racks[i] {
				// already in the set
				continue create_dc_info
			}
		}

		dcInfo.racks = append(dcInfo.racks, rack)
		dcInfo.rackCount++
	}

	// determine the replicas for all tokens
	for i := 0; i < ringSize; i++ {
		// reset the infos
		for _, dcInfo := range dcInfos {
			dcInfo.reset()
		}

		// size the array capacity assuming all hosts are added worst case
		replicas[i] = make([]*HostInfo, 0, ringSize)

		for j := 0; j < ringSize; j++ {
			// determine if done
			done := true
			for _, dcInfo := range dcInfos {
				if !dcInfo.done() {
					done = false
					break
				}
			}
			if done {
				break
			}

			candidate := hosts[(i+j)%ringSize]

			dc := candidate.DataCenter

			if dc == "" {
				// not included in the strategy
				continue
			}

			dcInfo, found := dcInfos[dc]
			if !found {
				// not included in the strategy
				continue
			}

			if dcInfo.done() {
				// all replicas from this dc for this token are fulfilled
				continue
			}

			var added bool

			// check the rack
			rack := candidate.Rack
			if rack == "" {
				// include this candidate since it has no rack
				replicas[i], added = hostSetAppend(replicas[i], candidate)
				if added {
					dcInfo.replicaCount++
				}
				continue
			}

			// check if all racks visited in this dc
			if dcInfo.visitedRackCount == dcInfo.rackCount {
				// include this candidate since all racks are visited already
				replicas[i], added = hostSetAppend(replicas[i], candidate)
				if added {
					dcInfo.replicaCount++
				}
				continue
			}

			// not all racks have been visited;
			// check if this rack has been visited already
			alreadyVisited := false
			for i := range dcInfo.visitedRacks {
				if rack == dcInfo.visitedRacks[i] {
					alreadyVisited = true
					break
				}
			}
			if alreadyVisited {
				// skip until after all racks visited
				dcInfo.skipped, _ = hostSetAppend(dcInfo.skipped, candidate)
				continue
			}

			// include this candidate, rack not visited
			replicas[i], added = hostSetAppend(replicas[i], candidate)
			if added {
				dcInfo.replicaCount++
			}

			// visit the rack
			dcInfo.visitedRacks = append(dcInfo.visitedRacks, rack)
			dcInfo.visitedRackCount++

			// check if all racks have been visited, if so, add the skipped hosts
			if dcInfo.visitedRackCount != dcInfo.rackCount {
				// not all racks visited, keep looking for hosts in non-visited racks
				continue
			}

			// add from skipped hosts now that all racks are visited
			// TODO use copy
			for _, host := range dcInfo.skipped {
				if dcInfo.done() {
					break
				}

				replicas[i], added = hostSetAppend(replicas[i], host)
				if added {
					dcInfo.replicaCount++
				}
			}
		}
	}

	// TODO compact the replicas in memory?

	return replicas
}
