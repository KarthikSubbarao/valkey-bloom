use crate::bloom_config;
use crate::commands::bloom_util::{ERROR};
use crate::commands::bloom_data_type::BLOOM_FILTER_TYPE2;
use bloomfilter::Bloom;
use redis_module::{Context, RedisError, RedisResult, RedisString, RedisValue};
use std::sync::atomic::Ordering;
use crate::commands::bloom_data_type::BloomFilterType2;

pub fn bloom_filter_add_value(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 3 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be added to the filter
    let item = &input_args[curr_cmd_idx];
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => {
            // Check if item exists.
            if val.bloom.check(&item) {
                return Ok(RedisValue::Integer(0));
            }

            // Add item.
            val.bloom.set(&item);
            val.num_items += 1;
            Ok(RedisValue::Integer(1))
        }
        None => {
            // Instantiate empty bloom filter.
            // TODO: Define false positive rate as a config.
            let fp_rate = 0.001;
            let mut bloom = Bloom::new_for_fp_rate(
                bloom_config::BLOOM_MAX_ITEM_COUNT.load(Ordering::Relaxed) as usize,
                fp_rate,
            );

            // Add item.
            bloom.set(item.as_slice());

            let value = BloomFilterType2 {
                bloom: bloom,
                num_items: 1,
                expansion: Some(bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as usize),
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, value) {
                Ok(_v) => {
                    Ok(RedisValue::Integer(1))
                }
                Err(_e) => Err(RedisError::Str("ERROR")),
            }
        }
    }
}

pub fn bloom_filter_exists(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 3 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the value to be checked whether it exists in the filter
    let item = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => {
            // Check if item exists.
            if val.bloom.check(&item) {
                return Ok(RedisValue::Integer(1));
            }
            Ok(RedisValue::Integer(0))
        }
        None => Ok(RedisValue::Integer(0)),
    }
}

pub fn bloom_filter_card(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc != 2 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    let filter_key = ctx.open_key(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(val) => Ok(RedisValue::Integer(val.num_items.try_into().unwrap())),
        None => Ok(RedisValue::Integer(0)),
    }
}

pub fn bloom_filter_reserve(ctx: &Context, input_args: &Vec<RedisString>) -> RedisResult {
    let argc = input_args.len();
    if argc < 4 || argc > 6 {
        return Err(RedisError::Str(ERROR));
    }
    let mut curr_cmd_idx = 0;
    let _cmd = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the filter name
    let filter_name = &input_args[curr_cmd_idx];
    curr_cmd_idx += 1;
    // Parse the error_rate
    let error_rate = match input_args[curr_cmd_idx].to_string_lossy().parse::<f64>() {
        Ok(num) if num >= 0.0 && num < 1.0  => num,
        _ => {
            return Err(RedisError::Str(ERROR));
        }
    };
    curr_cmd_idx += 1;
    // Parse the capacity
    let capacity = match input_args[curr_cmd_idx].to_string_lossy().parse::<usize>() {
        Ok(num) => num,
        _ => {
            return Err(RedisError::Str(ERROR));
        }
    };
    curr_cmd_idx += 1;
    let mut expansion = bloom_config::BLOOM_EXPANSION.load(Ordering::Relaxed) as usize;
    let mut noscaling = false; // DEFAULT
    let mut parse_expansion = false; // DEFAULT
    if argc > 4 {
        match input_args[curr_cmd_idx].to_string_lossy().to_uppercase().as_str() {
            "NONSCALING" if argc == 5 => {
                noscaling = true;
            }
            "EXPANSION" if argc == 6 => {
                curr_cmd_idx += 1;
                parse_expansion = true;
            }
            _ => {
                return Err(RedisError::Str(ERROR));
            }
        }
    }
    if parse_expansion {
        expansion = match input_args[curr_cmd_idx].to_string_lossy().parse::<usize>() {
            Ok(num) => num,
            _ => {
                return Err(RedisError::Str(ERROR));
            }
        };
    } else if noscaling {
        expansion = 1;
    }
    // If the filter does not exist, create one
    let filter_key = ctx.open_key_writable(filter_name);
    let my_value = match filter_key.get_value::<BloomFilterType2>(&BLOOM_FILTER_TYPE2) {
        Ok(v) => v,
        Err(_e) => {
            return Err(RedisError::Str(ERROR));
        }
    };
    match my_value {
        Some(_) => {
            Err(RedisError::Str("ERR item exists"))
        }
        None => {
            let bloom = Bloom::new_for_fp_rate(
                capacity,
                error_rate,
            );
            let value = BloomFilterType2 {
                bloom: bloom,
                num_items: 0,
                expansion: Some(expansion),
            };
            match filter_key.set_value(&BLOOM_FILTER_TYPE2, value) {
                Ok(_v) => {
                    Ok(RedisValue::SimpleStringStatic("OK"))
                }
                Err(_e) => Err(RedisError::Str("ERROR")),
            }
        }
    }
}
