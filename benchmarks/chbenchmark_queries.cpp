#include "chbenchmark.h"

#include "catalog.h"
#include "table.h"
#include "tpcc_const.h"
#include "tpcc_helper.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>

namespace {
typedef std::vector<char> Tuple;

template <typename T>
T value(const Tuple &tuple, table_t *table, int column) {
  T result;
  const int offset = table->get_schema()->get_field_index(column);
  memcpy(&result, tuple.data() + offset, sizeof(T));
  return result;
}

std::string text(const Tuple &tuple, table_t *table, int column) {
  Catalog *schema = table->get_schema();
  const int offset = schema->get_field_index(column);
  const int size = schema->get_field_size(column);
  std::string result(tuple.data() + offset, tuple.data() + offset + size);
  const size_t zero = result.find('\0');
  if (zero != std::string::npos) result.resize(zero);
  while (!result.empty() && result.back() == ' ') result.pop_back();
  return result;
}

bool starts(const std::string &s, const std::string &prefix) {
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}
bool ends(const std::string &s, const std::string &suffix) {
  return s.size() >= suffix.size() &&
         s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}
bool contains(const std::string &s, const std::string &needle) {
  return s.find(needle) != std::string::npos;
}

void mix(uint64_t &hash, uint64_t v) {
  hash ^= v + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
}
void mix(uint64_t &hash, double v) {
  uint64_t bits; memcpy(&bits, &v, sizeof(bits)); mix(hash, bits);
}
void mix(uint64_t &hash, const std::string &v) {
  mix(hash, std::hash<std::string>()(v));
}

struct Customer {
  uint64_t id, d, w; std::string last, city, state, phone; double balance;
};
struct Order {
  uint64_t id, c, d, w, entry, carrier, olcnt;
};
struct OrderLine {
  uint64_t o, d, w, number, item, supply, delivery, quantity; double amount;
};
struct Item { uint64_t id, price; std::string name, data; };
struct Stock { uint64_t item, w, quantity, ordercnt; };
struct Supplier { uint64_t key, nation; std::string name, address, phone, comment; };
struct Nation { uint64_t key, region; std::string name; };

uint64_t order_key(uint64_t w, uint64_t d, uint64_t o) {
  return orderlineKey(w, d, o);
}

struct DataSet {
  std::vector<Customer> customers;
  std::vector<Order> orders;
  std::vector<OrderLine> lines;
  std::vector<Item> items;
  std::vector<Stock> stocks;
  std::vector<Supplier> suppliers;
  std::vector<Nation> nations;
  std::unordered_set<uint64_t> new_orders;
  std::unordered_map<uint64_t, Customer> customer_by_key;
  std::unordered_map<uint64_t, Order> order_by_key;
  std::unordered_map<uint64_t, std::vector<OrderLine> > lines_by_order;
  std::unordered_map<uint64_t, Item> item_by_id;
  std::unordered_map<uint64_t, Stock> stock_by_key;
  std::unordered_map<uint64_t, Supplier> supplier_by_key;
  std::unordered_map<uint64_t, Nation> nation_by_key;
  std::unordered_map<uint64_t, std::string> region_by_key;
};
}

uint64_t CHBenchmarkTxnManager::execute_olap_operators(uint64_t q) {
  DataSet d;
  for (const Tuple &t : ch_data[CH_CUSTOMER_SLOT]) {
    Customer x = {value<uint64_t>(t,_ch_wl->t_customer,C_ID),
                  value<uint64_t>(t,_ch_wl->t_customer,C_D_ID),
                  value<uint64_t>(t,_ch_wl->t_customer,C_W_ID),
                  text(t,_ch_wl->t_customer,C_LAST), text(t,_ch_wl->t_customer,C_CITY),
                  text(t,_ch_wl->t_customer,C_STATE), text(t,_ch_wl->t_customer,C_PHONE),
                  value<double>(t,_ch_wl->t_customer,C_BALANCE)};
    d.customer_by_key[custKey(x.id,x.d,x.w)] = x; d.customers.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_ORDER_SLOT]) {
    Order x = {value<uint64_t>(t,_ch_wl->t_order,O_ID), value<uint64_t>(t,_ch_wl->t_order,O_C_ID),
               value<uint64_t>(t,_ch_wl->t_order,O_D_ID), value<uint64_t>(t,_ch_wl->t_order,O_W_ID),
               value<uint64_t>(t,_ch_wl->t_order,O_ENTRY_D), value<uint64_t>(t,_ch_wl->t_order,O_CARRIER_ID),
               value<uint64_t>(t,_ch_wl->t_order,O_OL_CNT)};
    d.order_by_key[order_key(x.w,x.d,x.id)] = x; d.orders.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_ORDERLINE_SLOT]) {
    OrderLine x = {value<uint64_t>(t,_ch_wl->t_orderline,OL_O_ID),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_D_ID),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_W_ID),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_NUMBER),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_I_ID),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_SUPPLY_W_ID),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_DELIVERY_D),
                   value<uint64_t>(t,_ch_wl->t_orderline,OL_QUANTITY),
                   value<double>(t,_ch_wl->t_orderline,OL_AMOUNT)};
    d.lines_by_order[order_key(x.w,x.d,x.o)].push_back(x); d.lines.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_NEWORDER_SLOT])
    d.new_orders.insert(order_key(value<uint64_t>(t,_ch_wl->t_neworder,NO_W_ID),
                                  value<uint64_t>(t,_ch_wl->t_neworder,NO_D_ID),
                                  value<uint64_t>(t,_ch_wl->t_neworder,NO_O_ID)));
  for (const Tuple &t : ch_data[CH_ITEM_SLOT]) {
    Item x = {value<uint64_t>(t,_ch_wl->t_item,I_ID), value<uint64_t>(t,_ch_wl->t_item,I_PRICE),
              text(t,_ch_wl->t_item,I_NAME), text(t,_ch_wl->t_item,I_DATA)};
    d.item_by_id[x.id] = x; d.items.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_STOCK_SLOT]) {
    Stock x = {value<uint64_t>(t,_ch_wl->t_stock,S_I_ID), value<uint64_t>(t,_ch_wl->t_stock,S_W_ID),
               value<uint64_t>(t,_ch_wl->t_stock,S_QUANTITY), value<uint64_t>(t,_ch_wl->t_stock,S_ORDER_CNT)};
    d.stock_by_key[stockKey(x.item,x.w)] = x; d.stocks.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_SUPPLIER_SLOT]) {
    Supplier x = {value<uint64_t>(t,_ch_wl->t_supplier,SU_SUPPKEY),
                  value<uint64_t>(t,_ch_wl->t_supplier,SU_NATIONKEY),
                  text(t,_ch_wl->t_supplier,SU_NAME), text(t,_ch_wl->t_supplier,SU_ADDRESS),
                  text(t,_ch_wl->t_supplier,SU_PHONE), text(t,_ch_wl->t_supplier,SU_COMMENT)};
    d.supplier_by_key[x.key] = x; d.suppliers.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_NATION_SLOT]) {
    Nation x = {value<uint64_t>(t,_ch_wl->t_nation,N_NATIONKEY),
                value<uint64_t>(t,_ch_wl->t_nation,N_REGIONKEY), text(t,_ch_wl->t_nation,N_NAME)};
    d.nation_by_key[x.key] = x; d.nations.push_back(x);
  }
  for (const Tuple &t : ch_data[CH_REGION_SLOT])
    d.region_by_key[value<uint64_t>(t,_ch_wl->t_region,R_REGIONKEY)] = text(t,_ch_wl->t_region,R_NAME);

  uint64_t hash = 1469598103934665603ULL;
  ch_result_rows = 0;
  auto emit = [&](uint64_t v) { mix(hash,v); ++ch_result_rows; };
  auto supplier_for = [&](const Stock &s) -> const Supplier * {
    auto it = d.supplier_by_key.find((s.w * s.item) % CH_SUPPLIER_COUNT);
    return it == d.supplier_by_key.end() ? NULL : &it->second;
  };
  auto nation_for = [&](uint64_t key) -> const Nation * {
    auto it = d.nation_by_key.find(key); return it == d.nation_by_key.end() ? NULL : &it->second;
  };

  switch (q) {
    case 1: {
      struct A { uint64_t qty=0,count=0; double amount=0; } a[16];
      for (const OrderLine &l:d.lines) if (l.delivery>20070102 && l.number<16) {
        a[l.number].qty+=l.quantity; a[l.number].amount+=l.amount; ++a[l.number].count;
      }
      for (uint64_t n=1;n<16;++n) if(a[n].count){mix(hash,n);mix(hash,a[n].qty);mix(hash,a[n].amount);emit(a[n].count);}
      break;
    }
    case 2: {
      std::unordered_map<uint64_t,uint64_t> minimum;
      for(const Stock&s:d.stocks){const Supplier*su=supplier_for(s);const Nation*n=su?nation_for(su->nation):NULL;
        if(!n||d.region_by_key[n->region]!="Europe")continue; auto it=minimum.find(s.item);
        if(it==minimum.end()||s.quantity<it->second)minimum[s.item]=s.quantity;}
      std::vector<std::tuple<std::string,std::string,uint64_t> > out;
      for(const Stock&s:d.stocks){auto ii=d.item_by_id.find(s.item);const Supplier*su=supplier_for(s);const Nation*n=su?nation_for(su->nation):NULL;
        if(ii!=d.item_by_id.end()&&su&&n&&ends(ii->second.data,"b")&&d.region_by_key[n->region]=="Europe"&&minimum[s.item]==s.quantity)
          out.push_back(std::make_tuple(n->name,su->name,s.item));}
      std::sort(out.begin(),out.end()); for(auto &r:out){mix(hash,std::get<0>(r));mix(hash,std::get<1>(r));emit(std::get<2>(r));} break;
    }
    case 3: {
      std::vector<std::pair<double,uint64_t> > out;
      for(const Order&o:d.orders){uint64_t k=order_key(o.w,o.d,o.id);auto c=d.customer_by_key.find(custKey(o.c,o.d,o.w));
        if(c==d.customer_by_key.end()||!starts(c->second.state,"A")||!d.new_orders.count(k)||o.entry<=20070102)continue;
        double rev=0;for(const OrderLine&l:d.lines_by_order[k])rev+=l.amount;out.push_back({-rev,o.entry});}
      std::sort(out.begin(),out.end());for(auto&r:out){mix(hash,-r.first);emit(r.second);}break;
    }
    case 4: { std::map<uint64_t,uint64_t> groups;for(const Order&o:d.orders){for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)])if(l.delivery>=o.entry){++groups[o.olcnt];break;}}
      for(auto&r:groups){mix(hash,r.first);emit(r.second);}break; }
    case 5: { std::map<std::string,double> revenue;for(const Order&o:d.orders){if(o.entry<20070102)continue;auto c=d.customer_by_key.find(custKey(o.c,o.d,o.w));if(c==d.customer_by_key.end()||c->second.state.empty())continue;
        for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)]){auto st=d.stock_by_key.find(stockKey(l.item,l.w));if(st==d.stock_by_key.end())continue;const Supplier*su=supplier_for(st->second);const Nation*n=su?nation_for(su->nation):NULL;
          if(su&&n&&su->nation==(uint64_t)(unsigned char)c->second.state[0]&&d.region_by_key[n->region]=="Europe")revenue[n->name]+=l.amount;}}
      std::vector<std::pair<double,std::string> > out;for(auto&r:revenue)out.push_back({-r.second,r.first});std::sort(out.begin(),out.end());for(auto&r:out){mix(hash,r.second);mix(hash,-r.first);++ch_result_rows;}break; }
    case 6: {double rev=0;for(const OrderLine&l:d.lines)if(l.delivery>=19990101&&l.delivery<20200101&&l.quantity>=1&&l.quantity<=100000)rev+=l.amount;mix(hash,rev);ch_result_rows=1;break;}
    case 7: {std::map<std::tuple<uint64_t,uint64_t,uint64_t>,double> g;for(const Order&o:d.orders){auto c=d.customer_by_key.find(custKey(o.c,o.d,o.w));if(c==d.customer_by_key.end()||c->second.state.empty())continue;uint64_t cn=(unsigned char)c->second.state[0];
        for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)]){auto st=d.stock_by_key.find(stockKey(l.item,l.supply));const Supplier*su=st==d.stock_by_key.end()?NULL:supplier_for(st->second);const Nation*sn=su?nation_for(su->nation):NULL;const Nation*cnn=nation_for(cn);
          if(sn&&cnn&&((sn->name=="Germany"&&cnn->name=="Cambodia")||(sn->name=="Cambodia"&&cnn->name=="Germany")))g[{su->nation,cn,o.entry/10000}]+=l.amount;}}
      for(auto&r:g){mix(hash,std::get<0>(r.first));mix(hash,std::get<1>(r.first));mix(hash,std::get<2>(r.first));mix(hash,r.second);++ch_result_rows;}break;}
    case 8: {std::map<uint64_t,std::pair<double,double> > g;for(const Order&o:d.orders){auto c=d.customer_by_key.find(custKey(o.c,o.d,o.w));if(c==d.customer_by_key.end()||c->second.state.empty())continue;const Nation*cn=nation_for((unsigned char)c->second.state[0]);if(!cn||d.region_by_key[cn->region]!="Europe")continue;
        for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)]){auto ii=d.item_by_id.find(l.item);auto st=d.stock_by_key.find(stockKey(l.item,l.supply));const Supplier*su=st==d.stock_by_key.end()?NULL:supplier_for(st->second);const Nation*sn=su?nation_for(su->nation):NULL;
          if(ii!=d.item_by_id.end()&&l.item<1000&&ends(ii->second.data,"b")&&sn){g[o.entry/10000].second+=l.amount;if(sn->name=="Germany")g[o.entry/10000].first+=l.amount;}}}
      for(auto&r:g){mix(hash,r.first);mix(hash,r.second.second? r.second.first/r.second.second:0);++ch_result_rows;}break;}
    case 9: {std::map<std::pair<std::string,uint64_t>,double>g;for(const Order&o:d.orders)for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)]){auto ii=d.item_by_id.find(l.item);auto st=d.stock_by_key.find(stockKey(l.item,l.supply));const Supplier*su=st==d.stock_by_key.end()?NULL:supplier_for(st->second);const Nation*n=su?nation_for(su->nation):NULL;if(ii!=d.item_by_id.end()&&ends(ii->second.data,"bb")&&n)g[{n->name,o.entry/10000}]+=l.amount;}
      for(auto&r:g){mix(hash,r.first.first);mix(hash,r.first.second);mix(hash,r.second);++ch_result_rows;}break;}
    case 10:{std::vector<std::pair<double,uint64_t> >out;for(const Order&o:d.orders){if(o.entry<20070102)continue;auto c=d.customer_by_key.find(custKey(o.c,o.d,o.w));if(c==d.customer_by_key.end())continue;double rev=0;for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)])if(o.entry<=l.delivery)rev+=l.amount;if(rev)out.push_back({-rev,c->second.id});}
      std::sort(out.begin(),out.end());for(auto&r:out){mix(hash,-r.first);emit(r.second);}break;}
    case 11:{std::unordered_map<uint64_t,uint64_t>g;uint64_t total=0;for(const Stock&s:d.stocks){const Supplier*su=supplier_for(s);const Nation*n=su?nation_for(su->nation):NULL;if(n&&n->name=="Germany"){g[s.item]+=s.ordercnt;total+=s.ordercnt;}}
      std::vector<std::pair<uint64_t,uint64_t> >out;for(auto&r:g)if(r.second>total*.005)out.push_back({r.second,r.first});std::sort(out.rbegin(),out.rend());for(auto&r:out){mix(hash,r.second);emit(r.first);}break;}
    case 12:{std::map<uint64_t,std::pair<uint64_t,uint64_t> >g;for(const Order&o:d.orders)for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)])if(o.entry<=l.delivery&&l.delivery<20200101){if(o.carrier==1||o.carrier==2)++g[o.olcnt].first;else ++g[o.olcnt].second;}
      for(auto&r:g){mix(hash,r.first);mix(hash,r.second.first);emit(r.second.second);}break;}
    case 13:{std::unordered_map<uint64_t,uint64_t>cnt;for(const Customer&c:d.customers)cnt[custKey(c.id,c.d,c.w)]=0;for(const Order&o:d.orders)if(o.carrier>8)++cnt[custKey(o.c,o.d,o.w)];std::map<uint64_t,uint64_t>dist;for(auto&r:cnt)++dist[r.second];
      std::vector<std::pair<uint64_t,uint64_t> >out;for(auto&r:dist)out.push_back({r.second,r.first});std::sort(out.rbegin(),out.rend());for(auto&r:out){mix(hash,r.second);emit(r.first);}break;}
    case 14:{double promo=0,total=0;for(const OrderLine&l:d.lines)if(l.delivery>=20070102&&l.delivery<20200102){auto i=d.item_by_id.find(l.item);if(i==d.item_by_id.end())continue;total+=l.amount;if(starts(i->second.data,"PR"))promo+=l.amount;}mix(hash,100.0*promo/(1.0+total));ch_result_rows=1;break;}
    case 15:{std::unordered_map<uint64_t,double>rev;for(const OrderLine&l:d.lines)if(l.delivery>=20070102){auto s=d.stock_by_key.find(stockKey(l.item,l.supply));if(s!=d.stock_by_key.end())rev[(s->second.w*s->second.item)%CH_SUPPLIER_COUNT]+=l.amount;}double mx=0;for(auto&r:rev)mx=std::max(mx,r.second);
      for(auto&r:rev)if(r.second==mx){auto su=d.supplier_by_key.find(r.first);if(su!=d.supplier_by_key.end()){mix(hash,su->second.name);mix(hash,r.second);++ch_result_rows;}}break;}
    case 16:{std::map<std::tuple<std::string,std::string,uint64_t>,std::set<uint64_t> >g;for(const Stock&s:d.stocks){auto i=d.item_by_id.find(s.item);const Supplier*su=supplier_for(s);if(i==d.item_by_id.end()||!su||starts(i->second.data,"zz")||contains(su->comment,"bad"))continue;g[{i->second.name,i->second.data.substr(0,3),i->second.price}].insert(su->key);}
      std::vector<uint64_t>counts;for(auto&r:g)counts.push_back(r.second.size());std::sort(counts.rbegin(),counts.rend());for(uint64_t c:counts)emit(c);break;}
    case 17:{std::unordered_set<uint64_t>eligible;for(const Item&i:d.items)if(ends(i.data,"b"))eligible.insert(i.id);std::unordered_map<uint64_t,std::pair<uint64_t,uint64_t> >avg;for(const OrderLine&l:d.lines)if(eligible.count(l.item)){avg[l.item].first+=l.quantity;++avg[l.item].second;}double sum=0;for(const OrderLine&l:d.lines){auto a=avg.find(l.item);if(a!=avg.end()&&a->second.second&&l.quantity<(double)a->second.first/a->second.second)sum+=l.amount;}mix(hash,sum/2.0);ch_result_rows=1;break;}
    case 18:{std::vector<std::tuple<double,uint64_t,uint64_t> >out;for(const Order&o:d.orders){double sum=0;for(const OrderLine&l:d.lines_by_order[order_key(o.w,o.d,o.id)])sum+=l.amount;if(sum>200)out.push_back(std::make_tuple(-sum,o.entry,o.id));}std::sort(out.begin(),out.end());for(auto&r:out){mix(hash,-std::get<0>(r));mix(hash,std::get<1>(r));emit(std::get<2>(r));}break;}
    case 19:{double rev=0;for(const OrderLine&l:d.lines){auto i=d.item_by_id.find(l.item);if(i==d.item_by_id.end()||l.quantity<1||l.quantity>10||i->second.price<1||i->second.price>400000)continue;bool ok=(ends(i->second.data,"a")&&(l.w==1||l.w==2||l.w==3))||(ends(i->second.data,"b")&&(l.w==1||l.w==2||l.w==4))||(ends(i->second.data,"c")&&(l.w==1||l.w==5||l.w==3));if(ok)rev+=l.amount;}mix(hash,rev);ch_result_rows=1;break;}
    case 20:{std::unordered_map<uint64_t,uint64_t>qty;for(const OrderLine&l:d.lines)if(l.delivery>20100523)qty[l.item]+=l.quantity;std::set<uint64_t>supplier_keys;for(const Stock&s:d.stocks){auto i=d.item_by_id.find(s.item);if(i!=d.item_by_id.end()&&starts(i->second.data,"co")&&2*s.quantity>qty[s.item])supplier_keys.insert((s.item*s.w)%CH_SUPPLIER_COUNT);}std::vector<std::string>names;for(uint64_t sk:supplier_keys){auto su=d.supplier_by_key.find(sk);if(su!=d.supplier_by_key.end()){const Nation*n=nation_for(su->second.nation);if(n&&n->name=="Germany")names.push_back(su->second.name);}}std::sort(names.begin(),names.end());for(auto&s:names){mix(hash,s);++ch_result_rows;}break;}
    case 21:{std::map<std::string,uint64_t>g;for(const OrderLine&l:d.lines){auto o=d.order_by_key.find(order_key(l.w,l.d,l.o));auto st=d.stock_by_key.find(stockKey(l.item,l.w));const Supplier*su=st==d.stock_by_key.end()?NULL:supplier_for(st->second);const Nation*n=su?nation_for(su->nation):NULL;if(o==d.order_by_key.end()||!su||!n||n->name!="Germany"||l.delivery<=o->second.entry)continue;bool later=false;for(const OrderLine&l2:d.lines_by_order[order_key(l.w,l.d,l.o)])if(l2.delivery>l.delivery){later=true;break;}if(!later)++g[su->name];}std::vector<std::pair<uint64_t,std::string> >out;for(auto&r:g)out.push_back({r.second,r.first});std::sort(out.rbegin(),out.rend());for(auto&r:out){mix(hash,r.second);emit(r.first);}break;}
    case 22:{double sum=0;uint64_t count=0;for(const Customer&c:d.customers)if(!c.phone.empty()&&c.phone[0]>='1'&&c.phone[0]<='7'&&c.balance>0){sum+=c.balance;++count;}double avg=count?sum/count:0;std::unordered_set<uint64_t>has_order;for(const Order&o:d.orders)has_order.insert(custKey(o.c,o.d,o.w));std::map<char,std::pair<uint64_t,double> >g;for(const Customer&c:d.customers)if(!c.phone.empty()&&c.phone[0]>='1'&&c.phone[0]<='7'&&c.balance>avg&&!has_order.count(custKey(c.id,c.d,c.w))&&!c.state.empty()){++g[c.state[0]].first;g[c.state[0]].second+=c.balance;}for(auto&r:g){mix(hash,(uint64_t)(unsigned char)r.first);mix(hash,r.second.first);mix(hash,r.second.second);++ch_result_rows;}break;}
    default: assert(false);
  }
  return hash;
}
