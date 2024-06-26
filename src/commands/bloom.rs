use crate::bloom_config;
use crate::bloom_config::BLOOM_EXPANSION_MAX;
use crate::bloom_config::BLOOM_MAX_ITEM_COUNT_MAX;
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE;
use crate::commands::bloom_util::{BloomFilterType, ERROR};
use std::sync::atomic::Ordering;
use valkey_module::{Context, ValkeyError, ValkeyResult, ValkeyString, ValkeyValue, VALKEY_OK};

// TODO: Replace string literals in error messages with static
// TODO: Check all int / usize casting.

pub fn bloom_filter_add_value(
    ctx: &Context,
    input_args: &[ValkeyString],
    multi: bool,
) -> ValkeyResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    match value {
        Some(bf) => {
            if !multi {
                let item = input_args[curr_cmd_idx].as_slice();
                return Ok(ValkeyValue::Integer(bf.add_item(item)));
            }
            let mut result = Vec::new();
            for item in input_args.iter().take(argc).skip(curr_cmd_idx) {
                result.push(ValkeyValue::Integer(bf.add_item(item.as_slice())));
            }
            Ok(ValkeyValue::Array(result))
        }
        None => {
            // Instantiate empty bloom filter.
            let fp_rate = 0.001;
            let capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as u32;
            let expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            let result = match multi {
                true => {
                    let mut result = Vec::new();
                    for item in input_args.iter().take(argc).skip(curr_cmd_idx) {
                        result.push(ValkeyValue::Integer(bf.add_item(item.as_slice())));
                    }
                    Ok(ValkeyValue::Array(result))
                }
                false => {
                    let item = input_args[curr_cmd_idx].as_slice();
                    Ok(ValkeyValue::Integer(bf.add_item(item)))
                }
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => result,
                Err(_) => Err(ValkeyError::Str(ERROR)),
            }
        }
    }
}

pub fn bloom_filter_exists(
    ctx: &Context,
    input_args: &[ValkeyString],
    multi: bool,
) -> ValkeyResult {
    let argc = input_args.len();
    if (!multi && argc != 3) || argc < 3 {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be checked whether it exists in the filter
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    if !multi {
        let item = input_args[curr_cmd_idx].as_slice();
        return Ok(bloom_filter_item_exists(value, item));
    }
    let mut result = Vec::new();
    while curr_cmd_idx < argc {
        let item = input_args[curr_cmd_idx].as_slice();
        result.push(bloom_filter_item_exists(value, item));
        curr_cmd_idx += 1;
    }
    Ok(ValkeyValue::Array(result))
}

fn bloom_filter_item_exists(value: Option<&BloomFilterType>, item: &[u8]) -> ValkeyValue {
    if let Some(val) = value {
        if val.item_exists(item) {
            return ValkeyValue::Integer(1);
        }
        // Item has not been added to the filter.
        return ValkeyValue::Integer(0);
    };
    // Key does not exist.
    ValkeyValue::Integer(0)
}

pub fn bloom_filter_card(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(ValkeyError::WrongArity);
    }
    let curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    match value {
        Some(val) => Ok(ValkeyValue::Integer(val.cardinality())),
        None => Ok(ValkeyValue::Integer(0)),
    }
}

pub fn bloom_filter_reserve(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if !(4..=6).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the error rate
    let fp_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f32>() {
        Ok(num) if (0.0..1.0).contains(&num) => num,
        _ => {
            return Err(ValkeyError::Str("ERR (0 < error rate range < 1)"));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
        Ok(num) if num > 0 && num < BLOOM_MAX_ITEM_COUNT_MAX => num,
        _ => {
            return Err(ValkeyError::Str("ERR Bad capacity"));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    if argc > 4 {
        match input_args[curr_cmd_idx]
            .to_string_lossy()
            .to_uppercase()
            .as_str()
        {
            "NONSCALING" if argc == 5 => {
                expansion = 0;
            }
            "EXPANSION" if argc == 6 => {
                curr_cmd_idx += 1;
                expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
                    _ => {
                        return Err(ValkeyError::Str("ERR bad expansion"));
                    }
                };
            }
            _ => {
                return Err(ValkeyError::Str(ERROR));
            }
        }
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    match value {
        Some(_) => Err(ValkeyError::Str("ERR item exists")),
        None => {
            let bloom = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bloom) {
                Ok(_v) => VALKEY_OK,
                Err(_) => Err(ValkeyError::Str(ERROR)),
            }
        }
    }
}

pub fn bloom_filter_insert(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    // At the very least, we need: BF.INSERT <key> ITEMS <item>
    if argc < 4 {
        return Err(ValkeyError::WrongArity);
    }
    let mut idx = 1;
    // Parse the filter name
    let filter_name = &input_args[idx];
    idx += 1;
    let mut fp_rate = 0.001;
    let mut capacity = bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as u32;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as u32;
    let mut nocreate = false;
    while idx < argc {
        match input_args[idx].to_string_lossy().to_uppercase().as_str() {
            "ERROR" if idx < (argc - 1) => {
                idx += 1;
                fp_rate = match input_args[idx].to_string_lossy().parse::<f32>() {
                    Ok(num) if (0.0..1.0).contains(&num) => num,
                    Ok(num) if !(0.0..1.0).contains(&num) => {
                        return Err(ValkeyError::Str("ERR (0 < error rate range < 1)"));
                    }
                    _ => {
                        return Err(ValkeyError::Str("ERR Bad error rate"));
                    }
                };
            }
            "CAPACITY" if idx < (argc - 1) => {
                idx += 1;
                capacity = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num < BLOOM_MAX_ITEM_COUNT_MAX => num,
                    _ => {
                        return Err(ValkeyError::Str("ERR Bad capacity"));
                    }
                };
            }
            "NOCREATE" => {
                nocreate = true;
            }
            "NONSCALING" => {
                expansion = 0;
            }
            "EXPANSION" if idx < (argc - 1) => {
                idx += 1;
                expansion = match input_args[idx].to_string_lossy().parse::<u32>() {
                    Ok(num) if num > 0 && num <= BLOOM_EXPANSION_MAX => num,
                    _ => {
                        return Err(ValkeyError::Str("ERR bad expansion"));
                    }
                };
            }
            "ITEMS" if idx < (argc - 1) => {
                idx += 1;
                break;
            }
            _ => {
                return Err(ValkeyError::WrongArity);
            }
        }
        idx += 1;
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    let mut result = Vec::new();
    match value {
        Some(bf) => {
            for item in input_args.iter().take(argc).skip(idx) {
                result.push(ValkeyValue::Integer(bf.add_item(item.as_slice())));
            }
            Ok(ValkeyValue::Array(result))
        }
        None => {
            if nocreate {
                return Err(ValkeyError::Str("ERR not found"));
            }
            let mut bf = BloomFilterType::new_reserved(fp_rate, capacity, expansion);
            for item in input_args.iter().take(argc).skip(idx) {
                result.push(ValkeyValue::Integer(bf.add_item(item.as_slice())));
            }
            match filter_key.set_value(&BLOOM_FILTER_TYPE, bf) {
                Ok(_) => Ok(ValkeyValue::Array(result)),
                Err(_) => Err(ValkeyError::Str(ERROR)),
            }
        }
    }
}

pub fn bloom_filter_info(ctx: &Context, input_args: &[ValkeyString]) -> ValkeyResult {
    let argc = input_args.len();
    if !(2..=3).contains(&argc) {
        return Err(ValkeyError::WrongArity);
    }
    let mut curr_cmd_idx = 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    let filter_key = ctx.open_key(filter_name);
    let value = match filter_key.get_value::<BloomFilterType>(&BLOOM_FILTER_TYPE) {
        Ok(v) => v,
        Err(_) => {
            return Err(ValkeyError::Str(ERROR));
        }
    };
    match value {
        Some(val) if argc == 3 => {
            match input_args[curr_cmd_idx]
                .to_string_lossy()
                .to_uppercase()
                .as_str()
            {
                "CAPACITY" => Ok(ValkeyValue::Integer(val.capacity())),
                "SIZE" => Ok(ValkeyValue::Integer(val.get_memory_usage() as i64)),
                "FILTERS" => Ok(ValkeyValue::Integer(val.filters.len() as i64)),
                "ITEMS" => Ok(ValkeyValue::Integer(val.cardinality())),
                "EXPANSION" => {
                    if val.expansion == 0 {
                        return Ok(ValkeyValue::Integer(-1));
                    }
                    Ok(ValkeyValue::Integer(val.expansion as i64))
                }
                _ => Err(ValkeyError::Str("ERR Invalid information value")),
            }
        }
        Some(val) if argc == 2 => {
            let mut result = vec![
                ValkeyValue::SimpleStringStatic("Capacity"),
                ValkeyValue::Integer(val.capacity()),
                ValkeyValue::SimpleStringStatic("Size"),
                ValkeyValue::Integer(val.get_memory_usage() as i64),
                ValkeyValue::SimpleStringStatic("Number of filters"),
                ValkeyValue::Integer(val.filters.len() as i64),
                ValkeyValue::SimpleStringStatic("Number of items inserted"),
                ValkeyValue::Integer(val.cardinality()),
                ValkeyValue::SimpleStringStatic("Expansion rate"),
            ];
            if val.expansion == 0 {
                result.push(ValkeyValue::Integer(-1));
            } else {
                result.push(ValkeyValue::Integer(val.expansion as i64));
            }
            Ok(ValkeyValue::Array(result))
        }
        _ => Err(ValkeyError::Str("ERR not found")),
    }
}
