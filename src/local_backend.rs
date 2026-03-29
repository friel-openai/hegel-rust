use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::LazyLock;

use ciborium::Value;
use hegel_core::choices::Choice;
use hegel_core::engine::{Engine, EngineError, value_conforms_to_schema};
use hegel_core::schema::{DataValue, Schema};

use crate::cbor_utils::{as_bool, as_text, as_u64, map_get};
use crate::test_case::labels;

#[derive(Debug)]
pub enum LocalBackendError {
    StopTest,
    InvalidRequest(String),
}

#[derive(Debug)]
struct CollectionState {
    min_size: usize,
    max_size: Option<usize>,
    p_continue: f64,
    count: usize,
    rejections: usize,
    drawn: bool,
    force_stop: bool,
}

#[derive(Debug, Default)]
struct PoolState {
    next_id: i128,
    values: Vec<i128>,
    removed: HashSet<i128>,
}

impl PoolState {
    fn add(&mut self) -> i128 {
        self.next_id += 1;
        self.values.push(self.next_id);
        self.next_id
    }

    fn active_values(&self) -> Vec<i128> {
        self.values
            .iter()
            .copied()
            .filter(|value| !self.removed.contains(value))
            .collect()
    }

    fn consume(&mut self, variable_id: i128) {
        self.removed.insert(variable_id);
        while self
            .values
            .last()
            .is_some_and(|value| self.removed.contains(value))
        {
            self.values.pop();
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalSpanRecord {
    pub label: u64,
    pub start: usize,
    pub end: usize,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
    pub discarded: bool,
}

fn integer_value(value: i128) -> Value {
    let value = i64::try_from(value).expect("local backend integer should fit in i64");
    Value::Integer(value.into())
}

static SMALLEST_POSITIVE_FLOAT: LazyLock<f64> = LazyLock::new(|| {
    let next = f64::from_bits(1);
    if next > 0.0 { next } else { f64::MIN_POSITIVE }
});

pub struct LocalBackend {
    engine: Engine,
    simplest: bool,
    next_collection_id: usize,
    collections: HashMap<String, CollectionState>,
    pools: Vec<PoolState>,
    replay_choices: VecDeque<Choice>,
    recorded_choices: Vec<Choice>,
    spans: Vec<LocalSpanRecord>,
    span_stack: Vec<usize>,
    forced_values: VecDeque<DataValue>,
    generate_requests: usize,
    first_observed_schema: Option<Schema>,
    first_observed_value: Option<DataValue>,
    observed_values: Vec<(Schema, DataValue)>,
}

impl LocalBackend {
    pub fn from_seed(seed: u64) -> Self {
        Self {
            engine: Engine::from_seed(seed),
            simplest: false,
            next_collection_id: 0,
            collections: HashMap::new(),
            pools: Vec::new(),
            replay_choices: VecDeque::new(),
            recorded_choices: Vec::new(),
            spans: Vec::new(),
            span_stack: Vec::new(),
            forced_values: VecDeque::new(),
            generate_requests: 0,
            first_observed_schema: None,
            first_observed_value: None,
            observed_values: Vec::new(),
        }
    }

    pub fn from_choices(choices: Vec<Choice>) -> Self {
        Self::from_seed_and_choices(0, choices)
    }

    pub fn from_seed_and_choices(seed: u64, choices: Vec<Choice>) -> Self {
        Self {
            engine: Engine::from_seed(seed),
            simplest: false,
            next_collection_id: 0,
            collections: HashMap::new(),
            pools: Vec::new(),
            replay_choices: choices.into(),
            recorded_choices: Vec::new(),
            spans: Vec::new(),
            span_stack: Vec::new(),
            forced_values: VecDeque::new(),
            generate_requests: 0,
            first_observed_schema: None,
            first_observed_value: None,
            observed_values: Vec::new(),
        }
    }

    pub fn simplest() -> Self {
        Self {
            engine: Engine::simplest(),
            simplest: true,
            next_collection_id: 0,
            collections: HashMap::new(),
            pools: Vec::new(),
            replay_choices: VecDeque::new(),
            recorded_choices: Vec::new(),
            spans: Vec::new(),
            span_stack: Vec::new(),
            forced_values: VecDeque::new(),
            generate_requests: 0,
            first_observed_schema: None,
            first_observed_value: None,
            observed_values: Vec::new(),
        }
    }

    pub fn force_first_value(&mut self, value: DataValue) {
        self.force_values(vec![value]);
    }

    pub fn force_values(&mut self, values: Vec<DataValue>) {
        self.forced_values = values.into();
    }

    pub fn observed_first_value(&self) -> Option<(Schema, DataValue)> {
        (self.generate_requests == 1)
            .then(|| {
                self.first_observed_schema
                    .clone()
                    .zip(self.first_observed_value.clone())
            })
            .flatten()
    }

    pub fn observed_values(&self) -> &[(Schema, DataValue)] {
        &self.observed_values
    }

    pub fn recorded_choices(&self) -> &[Choice] {
        &self.recorded_choices
    }

    pub fn spans(&self) -> &[LocalSpanRecord] {
        &self.spans
    }

    pub fn handle_request(&mut self, request: &Value) -> Result<Value, LocalBackendError> {
        let command = map_get(request, "command")
            .and_then(as_text)
            .ok_or_else(|| LocalBackendError::InvalidRequest("missing command".to_owned()))?;

        match command {
            "generate" => {
                let raw_schema = map_get(request, "schema").ok_or_else(|| {
                    LocalBackendError::InvalidRequest("missing schema".to_owned())
                })?;
                let schema = schema_from_cbor(raw_schema)?;
                self.generate_requests += 1;
                let replayed = self.replay_value_choice(&schema)?;
                let value = if let Some(forced) = self.forced_values.pop_front() {
                    if !value_conforms_to_schema(&forced, &schema) {
                        return Err(LocalBackendError::InvalidRequest(format!(
                            "forced value {forced:?} does not conform to schema {schema:?}"
                        )));
                    }
                    forced
                } else if let Some(replayed) = replayed {
                    replayed
                } else {
                    self.engine
                        .generate(&schema)
                        .map_err(map_engine_error_to_backend)?
                };

                if self.generate_requests == 1 {
                    self.first_observed_schema = Some(schema.clone());
                    self.first_observed_value = Some(value.clone());
                }
                self.observed_values.push((schema.clone(), value.clone()));

                let start = self.recorded_choices.len();
                self.record_choice_for_value(&schema, &value);
                self.synthesize_generated_spans(&schema, &value, start);

                Ok(data_value_to_cbor(&value))
            }
            "start_span" => {
                let label = map_get(request, "label").and_then(as_u64).ok_or_else(|| {
                    LocalBackendError::InvalidRequest("missing span label".to_owned())
                })?;
                self.start_span(label);
                Ok(Value::Null)
            }
            "stop_span" => {
                let discard = map_get(request, "discard")
                    .and_then(as_bool)
                    .unwrap_or(false);
                self.stop_span(discard);
                Ok(Value::Null)
            }
            "mark_complete" => Ok(Value::Null),
            "new_collection" => {
                let base_name = map_get(request, "name")
                    .and_then(as_text)
                    .unwrap_or("collection");
                let min_size = map_get(request, "min_size").and_then(as_u64).unwrap_or(0) as usize;
                let max_size = map_get(request, "max_size")
                    .and_then(as_u64)
                    .map(|v| v as usize);
                let average_size = collection_average_size(min_size, max_size)?;
                let p_continue = calc_p_continue(
                    (average_size - min_size as f64).max(0.0),
                    max_size.map(|size| size.saturating_sub(min_size) as f64),
                );
                let server_name = format!("{}_{}", base_name, self.next_collection_id);
                self.next_collection_id += 1;
                self.collections.insert(
                    server_name.clone(),
                    CollectionState {
                        min_size,
                        max_size,
                        p_continue,
                        count: 0,
                        rejections: 0,
                        drawn: false,
                        force_stop: false,
                    },
                );
                Ok(Value::Text(server_name))
            }
            "collection_more" => {
                let name = map_get(request, "collection")
                    .and_then(as_text)
                    .ok_or_else(|| {
                        LocalBackendError::InvalidRequest("missing collection name".to_owned())
                    })?
                    .to_owned();
                let (min_size, max_size, count, force_stop, p_continue) = {
                    let state = self.collections.get(&name).ok_or_else(|| {
                        LocalBackendError::InvalidRequest(format!("unknown collection {name}"))
                    })?;
                    (
                        state.min_size,
                        state.max_size,
                        state.count,
                        state.force_stop,
                        state.p_continue,
                    )
                };
                let should_continue = if min_size == max_size.unwrap_or(usize::MAX) {
                    count < min_size
                } else {
                    let forced_result = if force_stop {
                        Some(false)
                    } else if count < min_size {
                        Some(true)
                    } else if max_size.is_some_and(|max_size| count >= max_size) {
                        Some(false)
                    } else {
                        None
                    };
                    let replayed = match self.replay_choices.front() {
                        Some(Choice::Boolean(_)) => match self.replay_choices.pop_front() {
                            Some(Choice::Boolean(value)) => Some(value),
                            _ => unreachable!("front already matched boolean choice"),
                        },
                        _ => None,
                    };
                    if let Some(forced) = forced_result {
                        forced
                    } else if let Some(replayed) = replayed {
                        replayed
                    } else {
                        self.choose_boolean(p_continue, None)?
                    }
                };
                self.recorded_choices.push(Choice::Boolean(should_continue));

                let state = self.collections.get_mut(&name).ok_or_else(|| {
                    LocalBackendError::InvalidRequest(format!("unknown collection {name}"))
                })?;
                state.drawn = true;
                if should_continue {
                    state.count += 1;
                }
                Ok(Value::Bool(should_continue))
            }
            "collection_reject" => {
                let name = map_get(request, "collection")
                    .and_then(as_text)
                    .ok_or_else(|| {
                        LocalBackendError::InvalidRequest("missing collection name".to_owned())
                    })?
                    .to_owned();
                let state = self.collections.get_mut(&name).ok_or_else(|| {
                    LocalBackendError::InvalidRequest(format!("unknown collection {name}"))
                })?;
                if state.count == 0 {
                    return Err(LocalBackendError::InvalidRequest(format!(
                        "collection {name} has no element to reject"
                    )));
                }
                state.count -= 1;
                state.rejections += 1;
                if state.rejections > std::cmp::max(3, 2 * state.count) {
                    if state.count < state.min_size {
                        return Err(LocalBackendError::StopTest);
                    }
                    state.force_stop = true;
                }
                Ok(Value::Null)
            }
            "new_pool" => {
                let pool_id = self.pools.len();
                self.pools.push(PoolState::default());
                Ok(integer_value(pool_id as i128))
            }
            "pool_add" => {
                let pool_id = pool_id_from_request(request)?;
                let pool = self.pool_mut(pool_id)?;
                let variable_id = pool.add();
                Ok(integer_value(variable_id))
            }
            "pool_generate" => {
                let pool_id = pool_id_from_request(request)?;
                let consume = map_get(request, "consume")
                    .and_then(as_bool)
                    .unwrap_or(false);
                let active = {
                    let pool = self.pool_mut(pool_id)?;
                    pool.active_values()
                };
                if active.is_empty() {
                    return Err(LocalBackendError::StopTest);
                }
                let index = self.choose_usize(0, active.len() - 1)?;
                let variable_id = active[index];
                if consume {
                    let pool = self.pool_mut(pool_id)?;
                    pool.consume(variable_id);
                }
                Ok(integer_value(variable_id))
            }
            "pool_consume" => {
                let pool_id = pool_id_from_request(request)?;
                let variable_id = integer_from_request(request, "variable_id")?;
                let pool = self.pool_mut(pool_id)?;
                pool.consume(variable_id);
                Ok(Value::Null)
            }
            other => Err(LocalBackendError::InvalidRequest(format!(
                "unsupported local command {other}"
            ))),
        }
    }

    fn start_span(&mut self, label: u64) {
        let index = self.spans.len();
        let parent = self.span_stack.last().copied();
        self.spans.push(LocalSpanRecord {
            label,
            start: self.recorded_choices.len(),
            end: self.recorded_choices.len(),
            parent,
            children: Vec::new(),
            discarded: false,
        });
        if let Some(parent) = parent {
            self.spans[parent].children.push(index);
        }
        self.span_stack.push(index);
    }

    fn stop_span(&mut self, discarded: bool) {
        let Some(index) = self.span_stack.pop() else {
            return;
        };
        let span = &mut self.spans[index];
        span.end = self.recorded_choices.len();
        span.discarded = discarded;
    }

    fn synthesize_generated_spans(
        &mut self,
        schema: &Schema,
        value: &DataValue,
        start: usize,
    ) -> usize {
        match (schema, value) {
            (Schema::Const { .. }, _) => start,
            (Schema::Boolean { .. }, DataValue::Boolean(_))
            | (Schema::Integer { .. }, DataValue::Integer(_))
            | (Schema::Float { .. }, DataValue::Float(_))
            | (Schema::String { .. }, DataValue::String(_))
            | (Schema::Binary { .. }, DataValue::Binary(_)) => start + 1,
            (Schema::OneOf { options }, value) => {
                let index = self.spans.len();
                let parent = self.span_stack.last().copied();
                self.spans.push(LocalSpanRecord {
                    label: labels::ONE_OF,
                    start,
                    end: start,
                    parent,
                    children: Vec::new(),
                    discarded: false,
                });
                if let Some(parent) = parent {
                    self.spans[parent].children.push(index);
                }
                self.span_stack.push(index);
                let Some(option) = options
                    .iter()
                    .find(|option| value_conforms_to_schema(value, option))
                else {
                    self.span_stack.pop();
                    return start + 1;
                };
                let end = self.synthesize_generated_spans(option, value, start + 1);
                self.span_stack.pop();
                self.spans[index].end = end;
                end
            }
            (Schema::List { elements, .. }, DataValue::List(values)) => {
                let index = self.spans.len();
                let parent = self.span_stack.last().copied();
                self.spans.push(LocalSpanRecord {
                    label: labels::LIST,
                    start,
                    end: start,
                    parent,
                    children: Vec::new(),
                    discarded: false,
                });
                if let Some(parent) = parent {
                    self.spans[parent].children.push(index);
                }
                self.span_stack.push(index);

                let mut offset = start;
                for value in values {
                    offset += 1;
                    let child_index = self.spans.len();
                    self.spans.push(LocalSpanRecord {
                        label: labels::LIST_ELEMENT,
                        start: offset,
                        end: offset,
                        parent: Some(index),
                        children: Vec::new(),
                        discarded: false,
                    });
                    self.spans[index].children.push(child_index);
                    self.span_stack.push(child_index);
                    offset = self.synthesize_generated_spans(elements, value, offset);
                    self.span_stack.pop();
                    self.spans[child_index].end = offset;
                }
                offset += 1;

                self.span_stack.pop();
                self.spans[index].end = offset;
                offset
            }
            (Schema::Tuple { elements }, DataValue::Tuple(values)) => {
                let index = self.spans.len();
                let parent = self.span_stack.last().copied();
                self.spans.push(LocalSpanRecord {
                    label: labels::TUPLE,
                    start,
                    end: start,
                    parent,
                    children: Vec::new(),
                    discarded: false,
                });
                if let Some(parent) = parent {
                    self.spans[parent].children.push(index);
                }
                self.span_stack.push(index);
                let mut offset = start;
                for (schema, value) in elements.iter().zip(values) {
                    offset = self.synthesize_generated_spans(schema, value, offset);
                }
                self.span_stack.pop();
                self.spans[index].end = offset;
                offset
            }
            _ => start,
        }
    }

    fn choose_usize(&mut self, min: usize, max: usize) -> Result<usize, LocalBackendError> {
        if min > max {
            return Err(LocalBackendError::InvalidRequest(format!(
                "invalid range {min}..={max}"
            )));
        }
        let schema = Schema::Integer {
            min_value: Some(min as i64),
            max_value: Some(max as i64),
        };
        match self
            .engine
            .generate(&schema)
            .map_err(map_engine_error_to_backend)?
        {
            DataValue::Integer(value) => Ok(value as usize),
            other => Err(LocalBackendError::InvalidRequest(format!(
                "expected integer choice, got {other:?}"
            ))),
        }
    }

    fn choose_boolean(
        &mut self,
        p_continue: f64,
        forced: Option<bool>,
    ) -> Result<bool, LocalBackendError> {
        if let Some(forced) = forced {
            return Ok(forced);
        }
        if self.simplest {
            return Ok(false);
        }
        if p_continue <= 0.0 {
            return Ok(false);
        }
        if p_continue >= 1.0 {
            return Ok(true);
        }
        let draw = self.choose_usize(0, 999_999)?;
        Ok((draw as f64) < p_continue * 1_000_000.0)
    }

    fn pool_mut(&mut self, pool_id: usize) -> Result<&mut PoolState, LocalBackendError> {
        self.pools
            .get_mut(pool_id)
            .ok_or_else(|| LocalBackendError::InvalidRequest(format!("unknown pool id {pool_id}")))
    }
}

impl LocalBackend {
    fn replay_value_choice(
        &mut self,
        schema: &Schema,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        if let Schema::Const { value } = schema {
            return Ok(Some(value.clone()));
        }

        let Some(choice) = self.replay_choices.pop_front() else {
            return Ok(None);
        };

        let value = match (schema, choice) {
            (Schema::OneOf { options }, Choice::Integer(index)) => {
                let index = usize::try_from(index).map_err(|_| {
                    LocalBackendError::InvalidRequest(format!(
                        "replayed one_of index {index} is negative"
                    ))
                })?;
                let Some(option) = options.get(index) else {
                    return Err(LocalBackendError::InvalidRequest(format!(
                        "replayed one_of index {index} is out of range for {} options",
                        options.len()
                    )));
                };
                match self.replay_value_choice(option)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (Schema::Boolean { .. }, Choice::Boolean(value)) => DataValue::Boolean(value),
            (
                Schema::Integer {
                    min_value,
                    max_value,
                },
                Choice::Integer(value),
            ) => {
                let min_value = min_value.unwrap_or(i64::MIN);
                let max_value = max_value.unwrap_or(i64::MAX);
                if !(min_value..=max_value).contains(&value) {
                    return Err(LocalBackendError::InvalidRequest(format!(
                        "replayed integer {value} is outside {min_value}..={max_value}"
                    )));
                }
                DataValue::Integer(value)
            }
            (
                Schema::Float {
                    min_value,
                    max_value,
                    allow_nan,
                    allow_infinity,
                    ..
                },
                Choice::Float(value),
            ) => {
                if value.is_nan() && !allow_nan {
                    return Err(LocalBackendError::InvalidRequest(
                        "replayed float is NaN but schema disallows NaN".to_owned(),
                    ));
                }
                if value.is_infinite() && !allow_infinity {
                    return Err(LocalBackendError::InvalidRequest(
                        "replayed float is infinite but schema disallows infinity".to_owned(),
                    ));
                }
                if value.is_finite()
                    && !(min_value.unwrap_or(f64::NEG_INFINITY)
                        ..=max_value.unwrap_or(f64::INFINITY))
                        .contains(&value)
                {
                    return Err(LocalBackendError::InvalidRequest(format!(
                        "replayed float {value} is outside schema bounds"
                    )));
                }
                DataValue::Float(value)
            }
            (Schema::String { .. }, Choice::String(value)) => DataValue::String(value),
            (Schema::Binary { .. }, Choice::Bytes(value)) => DataValue::Binary(value),
            (
                Schema::Dict {
                    keys,
                    values,
                    min_size,
                    max_size,
                },
                first_choice,
            ) if matches!(keys.as_ref(), Schema::Integer { .. })
                && matches!(values.as_ref(), Schema::Integer { .. }) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_dict_choice(keys, values, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::Dict {
                    keys,
                    values,
                    min_size,
                    max_size,
                },
                first_choice,
            ) if matches!(keys.as_ref(), Schema::Integer { .. })
                && matches!(values.as_ref(), Schema::String { .. }) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_string_dict_choice(keys, values, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::Dict {
                    keys,
                    values,
                    min_size,
                    max_size,
                },
                first_choice,
            ) if matches!(keys.as_ref(), Schema::Boolean { .. })
                && matches!(values.as_ref(), Schema::Boolean { .. }) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_boolean_dict_choice(*min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(
                elements.as_ref(),
                Schema::Tuple { elements } if elements.iter().all(|element| matches!(element, Schema::Integer { .. }))
            ) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_tuple_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(
                elements.as_ref(),
                Schema::List {
                    elements,
                    ..
                } if matches!(elements.as_ref(), Schema::Integer { .. })
            ) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_list_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(
                elements.as_ref(),
                Schema::List {
                    elements,
                    ..
                } if matches!(elements.as_ref(), Schema::Boolean { .. })
            ) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_boolean_list_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(elements.as_ref(), Schema::OneOf { .. }) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_generic_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(elements.as_ref(), Schema::Boolean { .. }) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_boolean_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(elements.as_ref(), Schema::Float { .. }) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_float_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(elements.as_ref(), Schema::Integer { .. }) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                first_choice,
            ) if matches!(elements.as_ref(), Schema::String { .. }) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_string_list_choice(elements, *min_size, *max_size)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (Schema::Tuple { elements }, first_choice)
                if elements
                    .iter()
                    .all(|element| matches!(element, Schema::Integer { .. })) =>
            {
                self.replay_choices.push_front(first_choice);
                match self.replay_integer_tuple_choice(elements)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (Schema::Tuple { elements }, first_choice) => {
                self.replay_choices.push_front(first_choice);
                match self.replay_generic_tuple_choice(elements)? {
                    Some(value) => value,
                    None => return Ok(None),
                }
            }
            (_, choice) => {
                self.replay_choices.push_front(choice);
                return Ok(None);
            }
        };

        Ok(Some(value))
    }

    fn record_choice_for_value(&mut self, schema: &Schema, value: &DataValue) {
        match (schema, value) {
            (Schema::OneOf { options }, value) => {
                if let Some((index, option)) = options
                    .iter()
                    .enumerate()
                    .find(|(_, option)| value_conforms_to_schema(value, option))
                {
                    self.recorded_choices.push(Choice::Integer(index as i64));
                    self.record_choice_for_value(option, value);
                }
            }
            (Schema::Boolean { .. }, DataValue::Boolean(value)) => {
                self.recorded_choices.push(Choice::Boolean(*value));
            }
            (Schema::Integer { .. }, DataValue::Integer(value)) => {
                self.recorded_choices.push(Choice::Integer(*value));
            }
            (Schema::Float { .. }, DataValue::Float(value)) => {
                self.recorded_choices.push(Choice::Float(*value));
            }
            (Schema::String { .. }, DataValue::String(value)) => {
                self.recorded_choices.push(Choice::String(value.clone()));
            }
            (Schema::Binary { .. }, DataValue::Binary(value)) => {
                self.recorded_choices.push(Choice::Bytes(value.clone()));
            }
            (
                Schema::Dict {
                    keys,
                    values: dict_values,
                    min_size,
                    max_size,
                },
                DataValue::Dict(values),
            ) if matches!(keys.as_ref(), Schema::Integer { .. })
                && matches!(dict_values.as_ref(), Schema::Integer { .. }) =>
            {
                self.record_integer_dict_choices(keys, dict_values, *min_size, *max_size, values);
            }
            (
                Schema::Dict {
                    keys,
                    values: dict_values,
                    min_size,
                    max_size,
                },
                DataValue::Dict(values),
            ) if matches!(keys.as_ref(), Schema::Integer { .. })
                && matches!(dict_values.as_ref(), Schema::String { .. }) =>
            {
                self.record_integer_string_dict_choices(
                    keys,
                    dict_values,
                    *min_size,
                    *max_size,
                    values,
                );
            }
            (
                Schema::Dict {
                    keys,
                    values: dict_values,
                    min_size,
                    max_size,
                },
                DataValue::Dict(values),
            ) if matches!(keys.as_ref(), Schema::Boolean { .. })
                && matches!(dict_values.as_ref(), Schema::Boolean { .. }) =>
            {
                self.record_boolean_dict_choices(*min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(
                elements.as_ref(),
                Schema::Tuple { elements } if elements.iter().all(|element| matches!(element, Schema::Integer { .. }))
            ) =>
            {
                self.record_integer_tuple_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(
                elements.as_ref(),
                Schema::List {
                    elements,
                    ..
                } if matches!(elements.as_ref(), Schema::Integer { .. })
            ) =>
            {
                self.record_integer_list_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(
                elements.as_ref(),
                Schema::List {
                    elements,
                    ..
                } if matches!(elements.as_ref(), Schema::Boolean { .. })
            ) =>
            {
                self.record_boolean_list_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(elements.as_ref(), Schema::OneOf { .. }) => {
                self.record_generic_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(elements.as_ref(), Schema::Boolean { .. }) => {
                self.record_boolean_list_choices(*min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(elements.as_ref(), Schema::Float { .. }) => {
                self.record_float_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(elements.as_ref(), Schema::Integer { .. }) => {
                self.record_integer_list_choices(elements, *min_size, *max_size, values);
            }
            (
                Schema::List {
                    elements,
                    min_size,
                    max_size,
                    ..
                },
                DataValue::List(values),
            ) if matches!(elements.as_ref(), Schema::String { .. }) => {
                self.record_string_list_choices(elements, *min_size, *max_size, values);
            }
            (Schema::Tuple { elements }, DataValue::Tuple(values))
                if elements.len() == values.len()
                    && elements
                        .iter()
                        .all(|element| matches!(element, Schema::Integer { .. })) =>
            {
                self.record_integer_tuple_choices(elements, values);
            }
            (Schema::Tuple { elements }, DataValue::Tuple(values))
                if elements.len() == values.len() =>
            {
                self.record_generic_tuple_choices(elements, values);
            }
            _ => {}
        }
    }

    fn replay_integer_element(
        &self,
        schema: &Schema,
        value: i64,
    ) -> Result<DataValue, LocalBackendError> {
        let Schema::Integer {
            min_value,
            max_value,
        } = schema
        else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed integer element used a non-integer schema".to_owned(),
            ));
        };
        let min_value = min_value.unwrap_or(i64::MIN);
        let max_value = max_value.unwrap_or(i64::MAX);
        if !(min_value..=max_value).contains(&value) {
            return Err(LocalBackendError::InvalidRequest(format!(
                "replayed integer {value} is outside {min_value}..={max_value}"
            )));
        }
        Ok(DataValue::Integer(value))
    }

    fn replay_generic_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(true) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(false) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(value) = self.replay_value_choice(elements)? else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(value);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_boolean_list_choice(
        &mut self,
        _elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(true) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(false) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean list ended early".to_owned(),
                ));
            };
            let Choice::Boolean(value) = choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean list contained a non-boolean choice".to_owned(),
                ));
            };
            values.push(DataValue::Boolean(value));
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_boolean_list_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let Schema::List {
            elements: inner_elements,
            min_size: inner_min_size,
            max_size: inner_max_size,
            ..
        } = elements
        else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed nested boolean list used a non-list schema".to_owned(),
            ));
        };
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(value) =
                self.replay_boolean_list_choice(inner_elements, *inner_min_size, *inner_max_size)?
            else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(value);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_integer_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer list ended early".to_owned(),
                ));
            };
            let Choice::Integer(value) = choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer list contained a non-integer choice".to_owned(),
                ));
            };
            values.push(self.replay_integer_element(elements, value)?);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_integer_list_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let Schema::List {
            elements: inner_elements,
            min_size: inner_min_size,
            max_size: inner_max_size,
            ..
        } = elements
        else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed nested integer list used a non-list schema".to_owned(),
            ));
        };
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(value) =
                self.replay_integer_list_choice(inner_elements, *inner_min_size, *inner_max_size)?
            else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(value);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_float_element(
        &self,
        schema: &Schema,
        value: f64,
    ) -> Result<DataValue, LocalBackendError> {
        let Schema::Float {
            min_value,
            max_value,
            allow_nan,
            allow_infinity,
            ..
        } = schema
        else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed float element used a non-float schema".to_owned(),
            ));
        };
        if value.is_nan() {
            if !allow_nan {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed float element is NaN but schema disallows NaN".to_owned(),
                ));
            }
        } else if value.is_infinite() {
            if !allow_infinity {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed float element is infinite but schema disallows infinity".to_owned(),
                ));
            }
        } else if !(min_value.unwrap_or(f64::NEG_INFINITY)..=max_value.unwrap_or(f64::INFINITY))
            .contains(&value)
        {
            return Err(LocalBackendError::InvalidRequest(format!(
                "replayed float element {value} is outside schema bounds"
            )));
        }
        Ok(DataValue::Float(value))
    }

    fn replay_float_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed float list ended early".to_owned(),
                ));
            };
            let Choice::Float(value) = choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed float list contained a non-float choice".to_owned(),
                ));
            };
            values.push(self.replay_float_element(elements, value)?);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_string_element(
        &self,
        schema: &Schema,
        value: &str,
    ) -> Result<DataValue, LocalBackendError> {
        let Schema::String { min_size, max_size } = schema else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed string element used a non-string schema".to_owned(),
            ));
        };
        let len = value.chars().count();
        if len < *min_size || max_size.is_some_and(|max_size| len > max_size) {
            return Err(LocalBackendError::InvalidRequest(format!(
                "replayed string length {len} is outside {min_size}..={:?}",
                max_size
            )));
        }
        if value.contains('\0') {
            return Err(LocalBackendError::InvalidRequest(
                "replayed string element contains a null byte".to_owned(),
            ));
        }
        Ok(DataValue::String(value.to_owned()))
    }

    fn replay_string_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed string list ended early".to_owned(),
                ));
            };
            let Choice::String(value) = choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed string list contained a non-string choice".to_owned(),
                ));
            };
            values.push(self.replay_string_element(elements, &value)?);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn record_integer_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Integer(value) = value else {
                return;
            };
            if self.replay_integer_element(elements, *value).is_err() {
                return;
            }
            self.recorded_choices.push(Choice::Integer(*value));
        }

        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_generic_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }
        for (index, value) in values.iter().enumerate() {
            let _ = index;
            self.recorded_choices.push(Choice::Boolean(true));
            self.record_choice_for_value(elements, value);
        }
        self.recorded_choices.push(Choice::Boolean(false));
    }

    fn record_integer_list_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        let Schema::List {
            elements: inner_elements,
            min_size: inner_min_size,
            max_size: inner_max_size,
            ..
        } = elements
        else {
            return;
        };

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::List(values) = value else {
                return;
            };
            self.record_integer_list_choices(
                inner_elements,
                *inner_min_size,
                *inner_max_size,
                values,
            );
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_boolean_list_choices(
        &mut self,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }
        for (index, value) in values.iter().enumerate() {
            let _ = index;
            self.recorded_choices.push(Choice::Boolean(true));
            let DataValue::Boolean(value) = value else {
                return;
            };
            self.recorded_choices.push(Choice::Boolean(*value));
        }
        self.recorded_choices.push(Choice::Boolean(false));
    }

    fn record_boolean_list_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        let Schema::List {
            min_size: inner_min_size,
            max_size: inner_max_size,
            ..
        } = elements
        else {
            return;
        };

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::List(values) = value else {
                return;
            };
            self.record_boolean_list_choices(*inner_min_size, *inner_max_size, values);
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_float_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Float(value) = value else {
                return;
            };
            if self.replay_float_element(elements, *value).is_err() {
                return;
            }
            self.recorded_choices.push(Choice::Float(*value));
        }

        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_string_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::String(value) = value else {
                return;
            };
            if self.replay_string_element(elements, value).is_err() {
                return;
            }
            self.recorded_choices.push(Choice::String(value.clone()));
        }

        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn replay_integer_tuple_choice(
        &mut self,
        elements: &[Schema],
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::with_capacity(elements.len());
        for element in elements {
            let Some(choice) = self.replay_choices.pop_front() else {
                self.replay_choices = saved;
                return Ok(None);
            };
            let Choice::Integer(value) = choice else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(self.replay_integer_element(element, value)?);
        }
        Ok(Some(DataValue::Tuple(values)))
    }

    fn replay_generic_tuple_choice(
        &mut self,
        elements: &[Schema],
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut values = Vec::with_capacity(elements.len());
        for element in elements {
            let Some(value) = self.replay_value_choice(element)? else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(value);
        }
        Ok(Some(DataValue::Tuple(values)))
    }

    fn replay_integer_tuple_list_choice(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let Schema::Tuple { elements } = elements else {
            return Err(LocalBackendError::InvalidRequest(
                "replayed integer tuple list used a non-tuple schema".to_owned(),
            ));
        };
        let saved = self.replay_choices.clone();
        let mut values = Vec::new();

        loop {
            let count = values.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(value) = self.replay_integer_tuple_choice(elements)? else {
                self.replay_choices = saved;
                return Ok(None);
            };
            values.push(value);
        }

        Ok(Some(DataValue::List(values)))
    }

    fn replay_integer_dict_choice(
        &mut self,
        keys: &Schema,
        values: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut entries = Vec::new();

        loop {
            let count = entries.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(key_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer dict ended before key".to_owned(),
                ));
            };
            let Choice::Integer(key) = key_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer dict contained a non-integer key".to_owned(),
                ));
            };
            let key = self.replay_integer_element(keys, key)?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer dict contained a duplicate key".to_owned(),
                ));
            }

            let Some(value_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer dict ended before value".to_owned(),
                ));
            };
            let Choice::Integer(value) = value_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer dict contained a non-integer value".to_owned(),
                ));
            };
            let value = self.replay_integer_element(values, value)?;
            entries.push((key, value));
        }

        Ok(Some(DataValue::Dict(entries)))
    }

    fn replay_integer_string_dict_choice(
        &mut self,
        keys: &Schema,
        values: &Schema,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut entries = Vec::new();

        loop {
            let count = entries.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(key_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer string dict ended before key".to_owned(),
                ));
            };
            let Choice::Integer(key) = key_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer string dict contained a non-integer key".to_owned(),
                ));
            };
            let key = self.replay_integer_element(keys, key)?;
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer string dict contained a duplicate key".to_owned(),
                ));
            }

            let Some(value_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer string dict ended before value".to_owned(),
                ));
            };
            let Choice::String(value) = value_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed integer string dict contained a non-string value".to_owned(),
                ));
            };
            let value = self.replay_string_element(values, &value)?;
            entries.push((key, value));
        }

        Ok(Some(DataValue::Dict(entries)))
    }

    fn replay_boolean_dict_choice(
        &mut self,
        min_size: usize,
        max_size: Option<usize>,
    ) -> Result<Option<DataValue>, LocalBackendError> {
        let saved = self.replay_choices.clone();
        let mut entries = Vec::new();

        loop {
            let count = entries.len();
            let should_continue = if count < min_size {
                true
            } else if max_size.is_some_and(|max_size| count >= max_size) {
                false
            } else {
                let Some(choice) = self.replay_choices.pop_front() else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                let Choice::Boolean(should_continue) = choice else {
                    self.replay_choices = saved;
                    return Ok(None);
                };
                should_continue
            };

            if !should_continue {
                break;
            }

            let Some(key_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean dict ended before key".to_owned(),
                ));
            };
            let Choice::Boolean(key) = key_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean dict contained a non-boolean key".to_owned(),
                ));
            };
            let key = DataValue::Boolean(key);
            if entries.iter().any(|(existing, _)| existing == &key) {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean dict contained a duplicate key".to_owned(),
                ));
            }

            let Some(value_choice) = self.replay_choices.pop_front() else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean dict ended before value".to_owned(),
                ));
            };
            let Choice::Boolean(value) = value_choice else {
                return Err(LocalBackendError::InvalidRequest(
                    "replayed boolean dict contained a non-boolean value".to_owned(),
                ));
            };
            entries.push((key, DataValue::Boolean(value)));
        }

        Ok(Some(DataValue::Dict(entries)))
    }

    fn record_integer_tuple_choices(&mut self, elements: &[Schema], values: &[DataValue]) {
        for (element, value) in elements.iter().zip(values.iter()) {
            let DataValue::Integer(value) = value else {
                return;
            };
            if self.replay_integer_element(element, *value).is_err() {
                return;
            }
            self.recorded_choices.push(Choice::Integer(*value));
        }
    }

    fn record_generic_tuple_choices(&mut self, elements: &[Schema], values: &[DataValue]) {
        for (element, value) in elements.iter().zip(values.iter()) {
            self.record_choice_for_value(element, value);
        }
    }

    fn record_integer_tuple_list_choices(
        &mut self,
        elements: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[DataValue],
    ) {
        let Schema::Tuple { elements } = elements else {
            return;
        };

        for (index, value) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Tuple(values) = value else {
                return;
            };
            self.record_integer_tuple_choices(elements, values);
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_integer_dict_choices(
        &mut self,
        keys: &Schema,
        values_schema: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[(DataValue, DataValue)],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, (key, value)) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Integer(key) = key else {
                return;
            };
            let DataValue::Integer(value) = value else {
                return;
            };
            if self.replay_integer_element(keys, *key).is_err()
                || self.replay_integer_element(values_schema, *value).is_err()
            {
                return;
            }
            self.recorded_choices.push(Choice::Integer(*key));
            self.recorded_choices.push(Choice::Integer(*value));
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_integer_string_dict_choices(
        &mut self,
        keys: &Schema,
        values_schema: &Schema,
        min_size: usize,
        max_size: Option<usize>,
        values: &[(DataValue, DataValue)],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, (key, value)) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Integer(key) = key else {
                return;
            };
            let DataValue::String(value) = value else {
                return;
            };
            if self.replay_integer_element(keys, *key).is_err()
                || self.replay_string_element(values_schema, value).is_err()
            {
                return;
            }
            self.recorded_choices.push(Choice::Integer(*key));
            self.recorded_choices.push(Choice::String(value.clone()));
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }

    fn record_boolean_dict_choices(
        &mut self,
        min_size: usize,
        max_size: Option<usize>,
        values: &[(DataValue, DataValue)],
    ) {
        if values.len() < min_size || max_size.is_some_and(|max_size| values.len() > max_size) {
            return;
        }

        for (index, (key, value)) in values.iter().enumerate() {
            if index >= min_size {
                self.recorded_choices.push(Choice::Boolean(true));
            }
            let DataValue::Boolean(key) = key else {
                return;
            };
            let DataValue::Boolean(value) = value else {
                return;
            };
            self.recorded_choices.push(Choice::Boolean(*key));
            self.recorded_choices.push(Choice::Boolean(*value));
        }
        if max_size.is_none_or(|max_size| values.len() < max_size) {
            self.recorded_choices.push(Choice::Boolean(false));
        }
    }
}

fn pool_id_from_request(request: &Value) -> Result<usize, LocalBackendError> {
    Ok(integer_from_request(request, "pool_id")? as usize)
}

fn integer_from_request(request: &Value, field: &str) -> Result<i128, LocalBackendError> {
    match map_get(request, field) {
        Some(Value::Integer(value)) => Ok((*value).into()),
        _ => Err(LocalBackendError::InvalidRequest(format!(
            "missing integer field {field}"
        ))),
    }
}

fn map_engine_error_to_backend(error: EngineError) -> LocalBackendError {
    match error {
        EngineError::UniqueGenerationExhausted => LocalBackendError::StopTest,
        other => LocalBackendError::InvalidRequest(other.to_string()),
    }
}

fn collection_average_size(
    min_size: usize,
    max_size: Option<usize>,
) -> Result<f64, LocalBackendError> {
    let max_size_f64 = max_size.map(|value| value as f64).unwrap_or(f64::INFINITY);
    let average_size = (min_size.saturating_mul(2).max(min_size.saturating_add(5))) as f64;
    let bounded_average = average_size.min(0.5 * (min_size as f64 + max_size_f64));
    if bounded_average < min_size as f64 || bounded_average > max_size_f64 {
        return Err(LocalBackendError::InvalidRequest(format!(
            "invalid collection sizing min_size={min_size} max_size={max_size_f64} average_size={bounded_average}"
        )));
    }
    Ok(bounded_average)
}

fn calc_p_continue(desired_avg: f64, max_size: Option<f64>) -> f64 {
    if max_size == Some(desired_avg) {
        return 1.0;
    }

    let mut p_continue = 1.0 - 1.0 / (1.0 + desired_avg);
    if p_continue == 0.0 || max_size.is_none() {
        return p_continue.clamp(0.0, 1.0 - f64::EPSILON);
    }

    let max_size = max_size.expect("finite collection max size should be present");
    while p_continue_to_avg(p_continue, max_size) > desired_avg {
        p_continue -= 0.0001;
        if p_continue < *SMALLEST_POSITIVE_FLOAT {
            p_continue = *SMALLEST_POSITIVE_FLOAT;
            break;
        }
    }

    let mut hi = 1.0;
    while desired_avg - p_continue_to_avg(p_continue, max_size) > 0.01 {
        let mid = (p_continue + hi) / 2.0;
        if p_continue_to_avg(mid, max_size) <= desired_avg {
            p_continue = mid;
        } else {
            hi = mid;
        }
    }
    p_continue
}

fn p_continue_to_avg(p_continue: f64, max_size: f64) -> f64 {
    if p_continue >= 1.0 {
        return max_size;
    }
    (1.0 / (1.0 - p_continue) - 1.0) * (1.0 - p_continue.powf(max_size))
}

fn schema_from_cbor(value: &Value) -> Result<Schema, LocalBackendError> {
    let json = cbor_to_json(value)?;
    Schema::from_json_value(&json)
        .map_err(|error| LocalBackendError::InvalidRequest(error.to_string()))
}

fn cbor_to_json(value: &Value) -> Result<serde_json::Value, LocalBackendError> {
    match value {
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(value) => Ok(serde_json::Value::Bool(*value)),
        Value::Integer(value) => {
            let integer: i128 = (*value).into();
            if let Ok(value) = i64::try_from(integer) {
                Ok(serde_json::Value::Number(value.into()))
            } else if let Ok(value) = u64::try_from(integer) {
                Ok(serde_json::Value::Number(value.into()))
            } else {
                Err(LocalBackendError::InvalidRequest(format!(
                    "integer {integer} is out of range for local schema parsing"
                )))
            }
        }
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| {
                LocalBackendError::InvalidRequest(
                    "NaN and infinity are not supported in local schema parsing".to_owned(),
                )
            }),
        Value::Text(value) => Ok(serde_json::Value::String(value.clone())),
        Value::Bytes(bytes) => Ok(serde_json::Value::Array(
            bytes.iter().map(|b| serde_json::Value::from(*b)).collect(),
        )),
        Value::Array(values) => Ok(serde_json::Value::Array(
            values
                .iter()
                .map(cbor_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        Value::Map(entries) => {
            let mut map = serde_json::Map::with_capacity(entries.len());
            for (key, value) in entries {
                let Value::Text(key) = key else {
                    return Err(LocalBackendError::InvalidRequest(
                        "local schema parsing expects text map keys".to_owned(),
                    ));
                };
                map.insert(key.clone(), cbor_to_json(value)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Tag(_, _) => Err(LocalBackendError::InvalidRequest(
            "tagged values are not supported in local schema parsing".to_owned(),
        )),
        _ => Err(LocalBackendError::InvalidRequest(
            "unsupported CBOR value in schema".to_owned(),
        )),
    }
}

pub fn data_value_to_cbor(value: &DataValue) -> Value {
    match value {
        DataValue::Null => Value::Null,
        DataValue::Boolean(value) => Value::Bool(*value),
        DataValue::Integer(value) => Value::Integer((*value).into()),
        DataValue::Float(value) => Value::Float(*value),
        DataValue::String(value) => Value::Text(value.clone()),
        DataValue::Binary(value) => Value::Bytes(value.clone()),
        DataValue::List(values) | DataValue::Tuple(values) => {
            Value::Array(values.iter().map(data_value_to_cbor).collect())
        }
        DataValue::Dict(entries) => Value::Array(
            entries
                .iter()
                .map(|(key, value)| {
                    Value::Array(vec![data_value_to_cbor(key), data_value_to_cbor(value)])
                })
                .collect(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_collection_request(name: &str, min_size: u64) -> Value {
        Value::Map(vec![
            (
                Value::Text("command".to_owned()),
                Value::Text("new_collection".to_owned()),
            ),
            (Value::Text("name".to_owned()), Value::Text(name.to_owned())),
            (
                Value::Text("min_size".to_owned()),
                Value::Integer(min_size.into()),
            ),
        ])
    }

    #[test]
    fn open_ended_collection_reaches_large_sizes() {
        let mut saw_large = false;
        for seed in 0..1_000 {
            let mut backend = LocalBackend::from_seed(seed);
            let response = backend
                .handle_request(&new_collection_request("collection", 0))
                .expect("new_collection should succeed");
            let Value::Text(name) = response else {
                panic!("expected collection name, got {response:?}");
            };

            let mut count = 0usize;
            loop {
                let more = backend
                    .handle_request(&Value::Map(vec![
                        (
                            Value::Text("command".to_owned()),
                            Value::Text("collection_more".to_owned()),
                        ),
                        (
                            Value::Text("collection".to_owned()),
                            Value::Text(name.clone()),
                        ),
                    ]))
                    .expect("collection_more should succeed");
                let Value::Bool(more) = more else {
                    panic!("expected bool, got {more:?}");
                };
                if !more {
                    break;
                }
                count += 1;
            }
            if count >= 20 {
                saw_large = true;
                break;
            }
        }
        assert!(
            saw_large,
            "expected at least one open-ended collection with size >= 20"
        );
    }

    #[test]
    fn local_backend_records_explicit_and_synthesized_spans() {
        let mut backend = LocalBackend::from_choices(vec![
            Choice::Integer(2),
            Choice::Boolean(true),
            Choice::Boolean(false),
            Choice::Boolean(false),
        ]);

        backend
            .handle_request(&Value::Map(vec![
                (
                    Value::Text("command".to_owned()),
                    Value::Text("start_span".to_owned()),
                ),
                (
                    Value::Text("label".to_owned()),
                    Value::Integer(labels::FLAT_MAP.into()),
                ),
            ]))
            .expect("start_span should succeed");
        backend
            .handle_request(&Value::Map(vec![
                (
                    Value::Text("command".to_owned()),
                    Value::Text("generate".to_owned()),
                ),
                (
                    Value::Text("schema".to_owned()),
                    Value::Map(vec![
                        (
                            Value::Text("type".to_owned()),
                            Value::Text("integer".to_owned()),
                        ),
                        (
                            Value::Text("min_value".to_owned()),
                            Value::Integer(0.into()),
                        ),
                        (
                            Value::Text("max_value".to_owned()),
                            Value::Integer(10.into()),
                        ),
                    ]),
                ),
            ]))
            .expect("integer generate should succeed");
        backend
            .handle_request(&Value::Map(vec![
                (
                    Value::Text("command".to_owned()),
                    Value::Text("generate".to_owned()),
                ),
                (
                    Value::Text("schema".to_owned()),
                    Value::Map(vec![
                        (
                            Value::Text("type".to_owned()),
                            Value::Text("list".to_owned()),
                        ),
                        (Value::Text("unique".to_owned()), Value::Bool(false)),
                        (Value::Text("min_size".to_owned()), Value::Integer(1.into())),
                        (Value::Text("max_size".to_owned()), Value::Integer(1.into())),
                        (
                            Value::Text("elements".to_owned()),
                            Value::Map(vec![(
                                Value::Text("type".to_owned()),
                                Value::Text("boolean".to_owned()),
                            )]),
                        ),
                    ]),
                ),
            ]))
            .expect("list generate should succeed");
        backend
            .handle_request(&Value::Map(vec![
                (
                    Value::Text("command".to_owned()),
                    Value::Text("stop_span".to_owned()),
                ),
                (Value::Text("discard".to_owned()), Value::Bool(false)),
            ]))
            .expect("stop_span should succeed");

        let spans = backend.spans();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].label, labels::FLAT_MAP);
        assert_eq!(spans[0].start, 0);
        assert_eq!(spans[0].end, 4);
        assert_eq!(spans[0].children, vec![1]);
        assert_eq!(spans[1].label, labels::LIST);
        assert_eq!(spans[1].parent, Some(0));
        assert_eq!(spans[1].children, vec![2]);
        assert_eq!(spans[2].label, labels::LIST_ELEMENT);
    }
}
