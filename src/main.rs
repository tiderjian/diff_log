use std::borrow::BorrowMut;
use std::fs::File;
use std::io::BufReader;
use std::io::Lines;
use std::io::BufRead;
use strip_bom::*;
use serde_json::{self, Value};

fn main() {
    let mysql_lines = read_lines("mysql.log").unwrap();

    let mut sync_lines = read_lines("library_es_sync.log").unwrap();

    let mut context = Context::new(&mut sync_lines);
    for mysql_line in mysql_lines {
        if let Ok(mysql_sql) = mysql_line {
            let id = get_mysql_log_id(mysql_sql.as_str());
            let res = find_by_sync(id, &mut context);
            if !res {
                println!("not found id: {}; line: {}", id, mysql_sql);
            }
        }
    }

    // check_library_es_sync_sort();

    // check_mysql_log_sort();
}

struct Context<'a>{
    pub sync_lines: &'a mut Lines<BufReader<File>>,
    pub prev_line: String
}

impl<'a> Context<'a>{
    pub fn new(sync_lines: &'a mut Lines<BufReader<File>>) -> Context{
        Context{
            sync_lines: sync_lines,
            prev_line: "".to_string()
        }
    }
}

fn find_by_sync(id: i32, context: &mut Context) -> bool {
    if context.prev_line != "" {
        let prev_id = get_sync_log_id(context.prev_line.as_str());
        if prev_id == id {
            context.prev_line = "".to_string();
            return true;
        }

        if prev_id > id {
            return false;
        }
    }
    while let Some(next_line) = context.sync_lines.next() {
        if let Ok(next_line_str) = next_line {
            let next_id = get_sync_log_id(next_line_str.as_str());
            if next_id == id {
                context.prev_line = "".to_string();
                return true;
            }

            if next_id > id {
                context.prev_line = next_line_str;
                return false;
            }
        }
    }

    return false;
}

fn get_sync_log_id(line: &str) -> i32{
    let line_json: Value = serde_json::from_str(line).unwrap();
    let id = line_json["id"].as_i64().unwrap();
    id.try_into().unwrap()
}

fn get_mysql_log_id(line: &str) -> i32{
    let tmp = line.split(",").collect::<Vec<&str>>()[0].replace("\"", "");
    let num = tmp.strip_bom();
    return num.parse::<i32>().unwrap();
}

fn check_library_es_sync_sort(){
    let lines = read_lines("library_es_sync.log").unwrap();

    let mut pre_id = 0;
    for line in lines {
        if let Ok(json) = line {
            let id = get_sync_log_id(json.as_str());
            if id < pre_id {
                println!("{} < {}", id, pre_id);
                return ();
            }
            pre_id = id;
        }
    }

    println!("finished!");
}

fn check_mysql_log_sort() {
    let lines = read_lines("mysql.log").unwrap();

    let mut pre_id = 0;
    for line in lines {
        if let Ok(sql) = line {
            let id = get_mysql_log_id(sql.as_str());
            if id < pre_id {
                println!("{} < {}", id, pre_id);
                return ();
            }
            pre_id = id;
        }
    }

    println!("finished!");
}

fn read_lines(filename: &str) -> Result<Lines<BufReader<File>>, String>{
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);
    Ok(reader.lines())
}
