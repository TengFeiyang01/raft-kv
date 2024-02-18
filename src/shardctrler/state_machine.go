package shardctrler

import "sort"

type CtrlerStateMachine struct {
	Configs []Config
}

func NewCtrlerStateMachine() *CtrlerStateMachine {
	cf := &CtrlerStateMachine{Configs: make([]Config, 1)}
	cf.Configs[0] = DefaultConfig()
	return cf
}

func (csm *CtrlerStateMachine) Query(num int) (Config, Err) {
	if num < 0 || num >= len(csm.Configs) {
		return csm.Configs[len(csm.Configs)-1], OK
	}
	return csm.Configs[num], OK
}

// Join 加入新的 Group 到集群之中，需要处理加入之后的负载均衡问题
func (csm *CtrlerStateMachine) Join(groups map[int][]string) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 将新的 Group 加入到 Groups 中
	for gid, servers := range groups {
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}
	// 构造 gid -> shard 映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	// shards
	// shard -> gid
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 进行 Shard 迁移
	// gid     shard
	//  1      [0, 1, 4, 6]
	//  2      [2, 3]
	//  3      [5, 7]
	//  4      []
	//-------第一次移动-------
	// gid     shard
	//  1      [1, 4, 6]
	//  2      [2, 3]
	//  3      [5, 7]
	//  4      [0]
	//-------第二次移动-------
	// gid     shard
	//  1      [4, 6]
	//  2      [2, 3]
	//  3      [5, 7]
	//  4      [0, 1]
	for {
		maxGid, minGid := gidWithMaxShards(gidToShards), gidWithMinShards(gidToShards)
		if maxGid != 0 && len(gidToShards[maxGid])-len(gidToShards[minGid]) <= 1 {
			break
		}

		// 最少的 shard 的 gid 增加一个 gid
		gidToShards[minGid] = append(gidToShards[minGid], gidToShards[maxGid][0])
		// 最多的 shard 的 gid 减少一个 gid
		gidToShards[maxGid] = gidToShards[maxGid][1:]
	}

	// 得到新的 gid -> shard 信息之后， 存储到 shards 数组中
	var newShards [NShards]int
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)

	return OK
}

func (csm *CtrlerStateMachine) Leave(gids []int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}

	// 构造 gid -> shard 映射关系
	gidToShards := make(map[int][]int)
	for gid := range newConfig.Groups {
		gidToShards[gid] = make([]int, 0)
	}
	for shard, gid := range newConfig.Shards {
		gidToShards[gid] = append(gidToShards[gid], shard)
	}

	// 删除对应的 gid，并且将对应的 shard 暂存起来
	var unassignedShards []int
	for _, gid := range gids {
		// 如果 gid 在数组当中，就删除掉
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		// 取出对应的 shard
		if shards, ok := gidToShards[gid]; ok {
			unassignedShards = append(unassignedShards, shards...)
			delete(gidToShards, gid)
		}
	}

	var newShards [NShards]int
	// 重新分配被删除的 gid 对应的 shard
	if len(newConfig.Groups) != 0 {
		for _, shard := range unassignedShards {
			minGId := gidWithMinShards(gidToShards)
			gidToShards[minGId] = append(gidToShards[minGId], shard)
		}
	}

	// 重新存储 shard 数组对应的关系
	for gid, shards := range gidToShards {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	// 配置项保存
	newConfig.Shards = newShards
	csm.Configs = append(csm.Configs, newConfig)

	return OK
}
func (csm *CtrlerStateMachine) Move(shardId, gid int) Err {
	num := len(csm.Configs)
	lastConfig := csm.Configs[num-1]
	// 构建新的配置
	newConfig := Config{
		Num:    num,
		Shards: lastConfig.Shards,
		Groups: copyGroups(lastConfig.Groups),
	}
	newConfig.Shards[shardId] = gid
	csm.Configs = append(csm.Configs, newConfig)
	return OK
}

func copyGroups(groups map[int][]string) map[int][]string {
	newGroup := make(map[int][]string, len(groups))
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroup[gid] = newServers
	}
	return newGroup
}

func gidWithMaxShards(gidToShards map[int][]int) int {
	if shard, ok := gidToShards[0]; ok && len(shard) > 0 {
		return 0
	}

	// 为了让每个节点在调用的时候获取到的配置是一样的
	// 这里将 gid 进行排序，以确定遍历的顺序是确定的
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	maxGid, maxShards := -1, -1
	for _, gid := range gids {
		if len(gidToShards[gid]) > maxShards {
			maxGid, maxShards = gid, len(gidToShards[gid])
		}
	}
	return maxGid
}

func gidWithMinShards(gidToShards map[int][]int) int {
	var gids []int
	for gid := range gidToShards {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	minGid, minShards := -1, NShards+1
	for _, gid := range gids {
		if gid != 0 && len(gidToShards[gid]) < minShards {
			minGid, minShards = gid, len(gidToShards[gid])
		}
	}
	return minGid
}
