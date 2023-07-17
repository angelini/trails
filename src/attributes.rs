use std::collections::BTreeMap;

use arrow::datatypes::{DataType, Field};

pub enum AttributeType {
    Boolean,
    UInt64,
    String,
}

pub struct AttributeDefinition {
    pub name: String,
    pub data_type: AttributeType,
}

pub struct AttributeSchema {
    pub attrs: Vec<AttributeDefinition>,
}

impl AttributeSchema {
    pub fn new<S: Into<String>>(definitions: Vec<(S, AttributeType)>) -> Self {
        let attrs = definitions
            .into_iter()
            .map(|def| AttributeDefinition {
                name: def.0.into(),
                data_type: def.1,
            })
            .collect();
        AttributeSchema { attrs }
    }

    pub fn arrow_fields(&self) -> Vec<Field> {
        self.attrs
            .iter()
            .map(|attr| {
                let name = attr.name.clone();
                match attr.data_type {
                    AttributeType::Boolean => Field::new(name, DataType::Boolean, true),
                    AttributeType::UInt64 => Field::new(name, DataType::UInt64, true),
                    AttributeType::String => Field::new(name, DataType::Utf8, true),
                }
            })
            .collect()
    }
}

pub enum Attribute {
    Boolean(bool),
    UInt64(u64),
    String(String),
}

pub type AttributeValues = BTreeMap<usize, Attribute>;
