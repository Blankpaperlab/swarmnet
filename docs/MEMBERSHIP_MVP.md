# Membership MVP (Week 5)

Week 5 adds bootstrap discovery, gossip peer exchange, and a deterministic lightweight membership commit flow.

## Bootstrap

- Nodes start with `bootstrap_count` known bootstrap IDs.
- Non-bootstrap nodes initially know: `self + bootstrap set`.
- Bootstrap nodes gossip from tick 1 to spread peer knowledge.

## Gossip

- Each online node periodically gossips its peer list to `gossip_fanout` targets.
- Target selection is deterministic from `(node_id, tick, seed)`.
- Gossip carries:
  - known peers
  - committed `view_epoch`
  - committed member set

## Lightweight Membership Commit

- Nodes in majority-capable mode propose view changes.
- Proposal contains:
  - target epoch (`current_epoch + 1`)
  - effective tick (`tick + 1`)
  - sorted member set
  - deterministic proposal hash
- Eligible voters sign one deterministic winner proposal.
- Quorum threshold is majority of eligible voters.
- Winning cert is committed only at the next tick boundary.

## Majority-Commit Only

- During partition windows, minority nodes are forced into `safe_mode`.
- `safe_mode` nodes do not propose/vote/commit.
- Majority side continues cert progression.
- After partition heals, gossip propagates latest committed view to lagging nodes.

## Churn

- Deterministic churn cycles take batches of nodes offline temporarily.
- Rejoined nodes recover membership view via gossip.
