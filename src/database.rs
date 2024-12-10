use super::Error;

use super::traits::{KeyValueStore, Indexable};

use chrono::{Utc, DateTime};
use uuid::Uuid;

use std::path::PathBuf;
use std::collections::BTreeMap;
use std::cmp::Ordering;

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct F64 {inner: f64}

impl F64 {
    pub fn new(inner: f64) -> Self {
        F64{inner}
    }
}

impl std::hash::Hash for F64 {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {state.write(&self.inner.to_le_bytes())}
}
impl PartialEq for F64 {
    fn eq(&self, other: &Self) -> bool {matches!(self.inner.total_cmp(&other.inner), Ordering::Equal)}
}
impl Eq for F64 {}
impl PartialOrd for F64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {Some(self.cmp(other))}
}
impl Ord for F64 {
    fn cmp(&self, other: &Self) -> Ordering {self.inner.total_cmp(&other.inner)}
}
impl From<f64> for F64 {
    fn from(item: f64) -> F64 {F64::new(item)}
}

impl std::fmt::Display for F64 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialOrd, Ord)]
pub enum Value {
    I64(i64),
    U64(u64),
    Bytes(Vec<u8>),
    F64(F64),
    r#String(String),
    Bool(bool),
    Array(Vec<Value>)
}

impl Value {
    pub fn as_i64(&self) -> Option<&i64> {if let Value::I64(val) = &self {Some(val)} else {None}}
    pub fn as_u64(&self) -> Option<&u64> {if let Value::U64(val) = &self {Some(val)} else {None}}
    pub fn as_bytes(&self) -> Option<&Vec<u8>> {if let Value::Bytes(val) = &self {Some(val)} else {None}}
    pub fn as_f64(&self) -> Option<&F64> {if let Value::F64(val) = &self {Some(val)} else {None}}
    pub fn as_string(&self) -> Option<&String> {if let Value::r#String(val) = &self {Some(val)} else {None}}
    pub fn as_bool(&self) -> Option<&bool> {if let Value::Bool(val) = &self {Some(val)} else {None}}
    pub fn as_array(&self) -> Option<&Vec<Value>> {if let Value::Array(val) = &self {Some(val)} else {None}}

    pub fn contains(&self, other: &Value) -> Option<bool> {
        self.as_array().map(|v| v.contains(other))
    }

    pub fn starts_with(&self, other: &Value) -> Option<bool> {
        if let Some(other) = other.as_string() {
            self.as_string().map(|s| s.starts_with(other))
        } else {None}
    }

    fn cmp(&self, other: &Value, cmp_type: &CmpType) -> Option<bool> {
        if *cmp_type == CmpType::E {
            Some(self == other)
        } else {
            self.partial_cmp(other).map(|ordering| {
                match cmp_type {
                    CmpType::GT if (ordering as i8) > 0 => true,
                    CmpType::GTE if (ordering as i8) >= 0 => true,
                    CmpType::LT if (ordering as i8) < 0 => true,
                    CmpType::LTE if (ordering as i8) <= 0 => true,
                    _ => false
                }
            })
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::I64(val) => other.as_i64().map(|oval| val.eq(oval)),
            Value::U64(val) => other.as_u64().map(|oval| val.eq(oval)),
            Value::Bytes(val) => other.as_bytes().map(|oval| val.eq(oval)),
            Value::F64(val) => other.as_f64().map(|oval| val.eq(oval)),
            Value::r#String(val) => other.as_string().map(|oval| val.eq(oval)),
            Value::Bool(val) => other.as_bool().map(|oval| val.eq(oval)),
            Value::Array(val) => other.as_array().map(|oval| val.eq(oval))
        }.unwrap_or(false)
    }
}

impl Eq for Value {}

impl From<i64> for Value {fn from(v: i64) -> Self {Value::I64(v)}}
impl From<u64> for Value {fn from(v: u64) -> Self {Value::U64(v)}}
impl From<usize> for Value {fn from(v: usize) -> Self {Value::U64(v as u64)}}
impl From<DateTime<Utc>> for Value {fn from(v: DateTime<Utc>) -> Self {Value::U64(v.timestamp() as u64)}}
impl From<F64> for Value {fn from(v: F64) -> Self {Value::F64(v)}}
impl From<String> for Value {fn from(v: String) -> Self {Value::r#String(v)}}
impl From<&str> for Value {fn from(v: &str) -> Self {Value::r#String(v.to_string())}}
impl From<bool> for Value {fn from(v: bool) -> Self {Value::Bool(v)}}
impl From<Vec<u8>> for Value {fn from(v: Vec<u8>) -> Self {Value::Bytes(v)}}
impl<V: Into<Value>> From<Vec<V>> for Value {fn from(v: Vec<V>) -> Self {Value::Array(v.into_iter().map(|v| v.into()).collect())}}

impl std::fmt::Debug for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::I64(i) => write!(f, "{}", i),
            Value::U64(u) => write!(f, "{}", u),
            Value::Bytes(u) => write!(f, "{}", hex::encode(u)),
            Value::F64(_f) => write!(f, "{}", _f),
            Value::r#String(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Array(vec) => write!(f, "{:#?}", vec)
        }
    }
}


#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum CmpType { GT, GTE, E, LT, LTE }

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    Cmp(Value, CmpType),
    Contains(Value),
    StartsWith(Value),
    All(Vec<Filter>),
    Any(Vec<Filter>),
    Not(Box<Filter>)

}

impl Filter {
    pub fn filter(&self, item: &Value) -> Option<bool> {
        match self {
            Filter::Cmp(value, cmp_type) => item.cmp(value, cmp_type),
            Filter::Contains(value) => item.contains(value),
            Filter::StartsWith(value) => item.starts_with(value),
            Filter::All(filters) => {
                for filter in filters {
                    if let Some(b) = filter.filter(item) {
                        if !b {
                            return Some(true);
                        }
                    } else {return None;}
                }
                Some(false)
            },
            Filter::Any(filters) => {
                for filter in filters {
                    if let Some(b) = filter.filter(item) {
                        if b {
                            return Some(true);
                        }
                    } else {return None;}
                }
                Some(false)
               },
            Filter::Not(filter) => filter.filter(item).map(|f| !f)
        }
    }

    pub fn contains<V: Into<Value>>(val: V) -> Filter {
        Filter::Contains(val.into())
    }
    pub fn range<V: Into<Value>>(start: V, end: V) -> Filter {
        Filter::All(vec![Filter::Cmp(start.into(), CmpType::GTE), Filter::Cmp(end.into(), CmpType::LTE)])
    }
    pub fn new_not(filter: Filter) -> Filter {Filter::Not(Box::new(filter))}
    pub fn cmp<V: Into<Value>>(cmp: CmpType, val: V) -> Filter {Filter::Cmp(val.into(), cmp)}
    pub fn equal<V: Into<Value>>(val: V) -> Filter {Filter::Cmp(val.into(), CmpType::E)}
    pub fn is_equal(&self) -> bool {
        if let Filter::Cmp(_, t) = self {
            *t == CmpType::E
        } else {false}
    }
}

pub type Index = BTreeMap<String, Value>;

pub struct IndexBuilder {}
impl IndexBuilder {
    pub fn build<V: Into<Value>>(vec: Vec<(&str, V)>) -> Result<Index, Error> {
        let index = Index::from_iter(vec.into_iter().map(|(k, v)| (k.to_string(), v.into())));
        if index.contains_key("timestamp_stored") {
            Err(Error::err("", "'timestamp_stored' is a reserved index"))
        } else {Ok(index)}
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Filters(pub BTreeMap<String, Filter>);

impl Filters {
    pub fn new(vec: Vec<(&str, Filter)>) -> Filters {
        Filters(BTreeMap::from_iter(vec.into_iter().map(|(k, f)| (k.to_string(), f))))
    }
    pub fn add(&mut self, property: &str, ad_filter: Filter) {
        if let Some(filter) = self.0.get_mut(property) {
            if let Filter::All(ref mut filters) = filter {
                filters.push(ad_filter);
            } else {
                *filter = Filter::All(vec![filter.clone(), ad_filter]);
            }
        } else {
            self.0.insert(property.to_string(), ad_filter);
        }
    }
    pub fn combine(&self, b_filters: &Filters, or: bool) -> Filters {
        let mut filters = Vec::new();
        for (a_name, a_filter) in &self.0 {
            if let Some(b_filter) = b_filters.0.get(a_name) {
                let vec = vec![b_filter.clone(), a_filter.clone()];
                filters.push((a_name.to_string(), if or {Filter::Any(vec)} else {Filter::All(vec)}));
            } else {
                filters.push((a_name.to_string(), a_filter.clone()));
            }
        }
        for (b_name, b_filter) in &b_filters.0 {
            if !self.0.contains_key(b_name) {
                filters.push((b_name.to_string(), b_filter.clone()));
            }
        }
        Filters(BTreeMap::from_iter(filters))
    }
    pub fn filter(&self, index: &Index) -> bool {
        self.0.iter().all(|(prop, filter)| {
            if let Some(value) = index.get(prop) {
                filter.filter(value).unwrap_or(false)
            } else {false}
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum SortDirection {
  Descending = -1,
  Ascending = 1
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SortOptions {
    direction: SortDirection,
    property: String,
    limit: Option<usize>,
    cursor_key: Option<Vec<u8>>
}

impl SortOptions {
    pub fn new(property: &str) -> Self {
        SortOptions{
            direction: SortDirection::Ascending,
            property: property.to_string(),
            limit: None,
            cursor_key: None
        }
    }

    pub fn sort<T: Indexable>(&self, values: &mut [T]) -> Result<(), Error> {
        if values.iter().any(|a| !a.index().contains_key(&self.property)) {
            return Err(Error::err("", "Sort Property Not Found In All Values"));
        }
        values.sort_by(|a, b| {
            let (f, s) = if self.direction == SortDirection::Ascending {(a, b)} else {(b, a)};
            f.index().get(&self.property).unwrap()
            .partial_cmp(
                s.index().get(&self.property).unwrap()
            ).unwrap_or(Ordering::Equal)
        });
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct UuidKeyed<O: Indexable> {
    inner: O,
    uuid: Uuid
}

impl<O: Indexable> UuidKeyed<O> {
    pub fn new(inner: O) -> Self {
        UuidKeyed{inner, uuid: Uuid::new_v4()}
    }
    pub fn inner(self) -> O {self.inner}
}

impl<O: Indexable + Serialize + for<'a> Deserialize<'a>> Indexable for UuidKeyed<O> {
    const PRIMARY_KEY: &'static str = "uuid";
    const DEFAULT_SORT: &'static str = O::DEFAULT_SORT;
    fn primary_key(&self) -> Vec<u8> {self.uuid.as_bytes().to_vec()}
    fn secondary_keys(&self) -> Index {
        let mut index = self.inner.secondary_keys();
        index.insert(
            O::PRIMARY_KEY.to_string(),
            self.inner.primary_key().into()
        );
        index
    }
}

#[derive(Clone)]
pub struct Database {
    store: Box<dyn KeyValueStore>,
    location: PathBuf
}

pub const MAIN: &str = "___main___";
pub const INDEX: &str = "___index___";
pub const ALL: &str = "ALL";

impl Database {
    pub fn location(&self) -> PathBuf {self.location.clone()}
    pub async fn new<KVS: KeyValueStore + 'static>(location: PathBuf) -> Result<Self, Error> {
        Ok(Database{store: Box::new(KVS::new(location.clone()).await?), location})
    }

    pub async fn get_raw(&self, pk: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        Ok(if let Some(db) = self.store.get_partition(&PathBuf::from(MAIN)).await {
            db.get(pk).await?
        } else {None})
    }

    pub async fn get<I: Indexable + for<'a> Deserialize<'a>>(&self, pk: &[u8]) -> Result<Option<I>, Error> {
        Ok(self.get_raw(pk).await?.map(|item| {
            serde_json::from_slice::<I>(&item)
        }).transpose()?)
    }

    pub async fn get_all<I: Indexable + for<'a> Deserialize<'a>>(&self) -> Result<Vec<I>, Error> {
        Ok(if let Some(db) = self.store.get_partition(&PathBuf::from(MAIN)).await {
            db.values().await?.into_iter().map(|item| Ok::<I, Error>(serde_json::from_slice::<I>(&item)?)).collect::<Result<Vec<I>, Error>>()?
        } else {Vec::new()})
    }

    pub async fn keys(&self) -> Result<Vec<Vec<u8>>, Error> {
        Ok(if let Some(db) = self.store.get_partition(&PathBuf::from(MAIN)).await {
            db.keys().await?
        } else {Vec::new()})
    }

    async fn add(partition: &dyn KeyValueStore, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
        if let Some(values) = partition.get(key).await? {
            let mut values: Vec<Vec<u8>> = serde_json::from_slice(&values)?;
            values.push(value);
            partition.set(key, &serde_json::to_vec(&values)?).await?;
        } else {
            partition.set(key, &serde_json::to_vec(&vec![value])?).await?;
        }
        Ok(())
    }

    async fn remove(partition: &dyn KeyValueStore, key: &[u8], value: &[u8]) -> Result<(), Error> {
        if let Some(values) = partition.get(key).await? {
            let mut values: Vec<Vec<u8>> = serde_json::from_slice(&values)?;
            values.retain(|v| v != value);
            if !values.is_empty() {
                partition.set(key, &serde_json::to_vec(&values)?).await?;
            } else {
                partition.delete(key).await?;
            }
        }
        Ok(())
    }

    pub async fn set<I: Indexable + Serialize>(&self, item: &I) -> Result<(), Error> {
        let pk = item.primary_key();
        self.delete(&pk).await?;
        let db = &self.store;
        let mut keys = item.secondary_keys();
        if keys.contains_key(I::PRIMARY_KEY) || keys.contains_key("timestamp_stored") {
            return Err(Error::err("", &format!("'{}' and 'timestamp_stored' are reserved indexes", I::PRIMARY_KEY)));
        }
        keys.insert(I::PRIMARY_KEY.to_string(), pk.clone().into());
        keys.insert("timestamp_stored".to_string(), Utc::now().into());
        db.partition(PathBuf::from(MAIN)).await?.set(&pk, &serde_json::to_vec(item)?).await?;
        db.partition(PathBuf::from(INDEX)).await?.set(&pk, &serde_json::to_vec(&keys)?).await?;
        for (key, value) in keys.iter() {
            let partition = db.partition(PathBuf::from(&format!("__{}__", key))).await?;
            let value = serde_json::to_vec(&value)?;
            Self::add(&*partition, &value, pk.clone()).await?;
            Self::add(&*partition, ALL.as_bytes(), pk.clone()).await?;
        }
        Ok(())
    }

    pub async fn delete(&self, pk: &[u8]) -> Result<(), Error> {
        let db = &self.store;
        db.partition(PathBuf::from(MAIN)).await?.delete(pk).await?;
        let index_db = db.partition(PathBuf::from(INDEX)).await?;
        if let Some(index) = index_db.get(pk).await? {
            index_db.delete(pk).await?;
            let keys: Index = serde_json::from_slice(&index)?;
            for (key, value) in keys.iter() {
                let partition = db.partition(PathBuf::from(&format!("__{}__", key))).await?;
                let value = serde_json::to_vec(value)?;
                Self::remove(&*partition, &value, pk).await?;
                Self::remove(&*partition, ALL.as_bytes(), pk).await?;
            }
        }
        Ok(())
    }

    pub async fn clear(&self) -> Result<(), Error> {
        self.store.clear().await?;
        Ok(())
    }

    pub async fn query<I: Indexable + for<'a> Deserialize<'a>>(
        &self,
        filters: &Filters,
        sort_options: Option<SortOptions>
    ) -> Result<(Vec<I>, Option<Vec<u8>>), Error> {
        let sort_options = sort_options.unwrap_or(SortOptions::new(I::DEFAULT_SORT));
        let none = || Ok((Vec::new(), None));
        let db = &self.store;

        if let Some(pk) = filters.0.iter().find_map(|(p, f)| {
            if p == I::PRIMARY_KEY {
                if let Filter::Cmp(Value::Bytes(value), CmpType::E) = f {
                    return Some(value);
                }
            }
            None
        }) {
            return Ok((self.get(pk).await?.map(|r| vec![r]).unwrap_or(vec![]), None));
        }


        let db_filters: Vec<String> = filters.0.iter().filter_map(|(p, f)|
            Some(p.to_string()).filter(|_| f.is_equal())
        ).collect();

        let partition_name = PathBuf::from(format!("__{}__",
            db_filters.first().unwrap_or(&sort_options.property)
        ));
        let partition = if let Some(p) = db.get_partition(&partition_name).await {p} else {return none();};
        let index = if let Some(p) = db.get_partition(&PathBuf::from(INDEX)).await {p} else {return none();};

        let all = if let Some(p) = partition.get(ALL.as_bytes()).await? {
            serde_json::from_slice::<Vec<Vec<u8>>>(&p)?
        } else {return none();};

        let mut values: Vec<(Vec<u8>, Value)> = vec![];
        for pk in all {
            let keys = index.get(&pk).await?.ok_or(
                Error::err("database.query", "Indexed value not found in index")
            )?;
            let keys = serde_json::from_slice::<Index>(&keys)?;
            if filters.filter(&keys) {
                let main = db.get_partition(&PathBuf::from(MAIN)).await.ok_or(
                    Error::err("database.query", "Indexed value not found in main")
                )?;
                let sp = keys.get(&sort_options.property).ok_or(
                    Error::err("database.query", "Sort property not found in matched value")
                )?.clone();

                if let Some(i) = main.get(&pk).await? {
                    values.push((i, sp));
                }
            }
        }

        values.sort_by(|a, b| {
            if sort_options.direction == SortDirection::Ascending {
                a.1.partial_cmp(&b.1).unwrap_or(Ordering::Equal)
            } else {
                b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal)
            }
        });

        let start = sort_options.cursor_key.and_then(|ck|
            values.iter().position(|(i, _)| *i == ck).map(|p| p+1)
        ).unwrap_or(0);
        let end = sort_options.limit.map(|l| start+l).unwrap_or(values.len());
        let cursor = if end != values.len() {Some(values[end].0.clone())} else {None};
        if start == end {
            Ok((Vec::new(), None))
        } else {
            Ok((values[start..end].iter().cloned().flat_map(|(item, _)|
                serde_json::from_slice(&item).ok()
            ).collect::<Vec<I>>(),
            cursor))
        }
    }

    pub async fn debug(&self) -> Result<String, Error> {
        Ok(format!("{:#?}",
            (
                self.location(),
                if let Some(index) = self.store.get_partition(&PathBuf::from(INDEX)).await {
                    let mut index = index.values().await?.into_iter().map(|keys|
                        Ok(serde_json::from_slice::<Index>(&keys)?)
                    ).collect::<Result<Vec<Index>, Error>>()?;
                    index.sort_by_key(|i| i.get("timestamp_stored").unwrap().clone());
                    index
                } else {Vec::new()}
            )
        ))
    }
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Database")
        .field("location", &self.location())
        .finish()
    }
}
