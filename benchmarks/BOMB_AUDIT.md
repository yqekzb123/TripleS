# Distributed BoMB workload audit

Sources:

- Nemoto et al., *Oze: Decentralized Graph-based Concurrency Control for
  Long-running Update Transactions*, PVLDB 18(8), 2025 / arXiv 2210.04179.
- Official CCBench artifact, `jnmt/ccbench`, branch `vldb-paper`, especially
  `include/bomb.hh`, `include/bomb_static.hh`,
  `include/sbomb_deterministic.hh`, and `include/dbomb_deterministic.hh`.

## Schema

The paper defines seven logical tables:

1. `factory(id, name)`
2. `item(id, name, type)`
3. `product(factory_id, item_id, quantity)`
4. `bom(parent_item_id, child_item_id, quantity)`
5. `material_cost(factory_id, item_id, stock_quantity, stock_amount)`
6. `result_cost(factory_id, item_id, cost)`
7. `journal_voucher(voucher_id, date, debit, credit, amount, description)`

The CCBench artifact omits physical `factory` and `item` storage because their
identifiers are generated/cached outside transactions. It stores the other five
tables as `ItemManufacturingMaster`, `ItemConstructionMaster`,
`MaterialCostMaster`, `ProductCostMaster`, and `JournalVoucher`.

## Transactions

- **L1 / update-product-cost**: choose one factory; scan all products made at
  that factory; recursively scan `bom` for every product; read the
  `material_cost` row for every raw-material leaf; calculate each product cost;
  update one `result_cost` row per product. Default `target-products=100` gives
  roughly 20,000 reads/scans and 100 writes in the paper.
- **S1 / update-material-cost**: choose a factory and `target-materials` raw
  materials; read-modify-write their `material_cost` records.
- **S2 / issue-journal-voucher**: choose a factory; scan all its
  `result_cost` records; insert one journal-voucher record per product.
- **S3 / change-product**: choose and delete one existing product at a factory;
  allocate a new product id; choose `material-trees-per-product` root materials;
  insert the new root `bom` edges and product record.
- **S4 / change-raw-material**: choose a material/raw-material edge from a BoM
  tree and a replacement raw material; delete the old edge and insert the new
  edge.
- **S5 / change-product-quantity**: choose a factory and one of its products;
  update the corresponding `product.quantity`.

Static BoM runs L1/S1/S2 with a 50/50 S1/S2 short mix. Dynamic BoM adds
S3/S4/S5; the paper's default short mix is 45/45/1/1/8.

## Default scale

- factories: 8
- product types: 72,000
- material types: 198,000
- raw-material types: 75,000
- material trees per product: 5
- material tree size: 10
- raw materials per leaf: 3
- target products per factory: 100
- target materials per S1: 1

## Paper/artifact differences and deterministic execution

- The paper describes seven logical tables; the artifact physically stores five
  and treats factory/item identifiers as cached input.
- The paper describes `stock_amount`; the artifact field is named
  `mc_stock_price`, but L1 computes unit cost as price/quantity.
- The current artifact defaults expose both dedicated-thread rates and a mixed
  request dispatcher. The current paper specifies 50/50 static and
  45/45/1/1/8 dynamic short mixes.
- Static deterministic BoMB caches the immutable BoM and therefore has a known
  read/write set.
- Dynamic deterministic BoMB does reconnaissance, locks the planned keys, then
  re-reads and validates product membership and BoM topology. A mismatch aborts
  and retries. It is not a workload whose exact read/write set is intrinsically
  known without planning and validation.

This port follows the latter rule in a protocol-independent workload planner:
every algorithm consumes the same planned access set and topology-version
guards. A mismatch must abort before business writes and retry with a newly
planned set.
