extern crate mysqlclient_sys as ffi;

use mysql::MysqlType;
use std::mem;
use std::os::{raw as libc};

pub struct Binds {
    data: Vec<Option<Vec<u8>>>,
    lengths: Vec<libc::c_ulong>,
    is_nulls: Vec<ffi::my_bool>,
    mysql_binds: Vec<ffi::MYSQL_BIND>,
}

impl Binds {
    pub fn from_input_data(input: Vec<(MysqlType, Option<Vec<u8>>)>) -> Self {
        let lengths = input.iter().map(|x| match *x {
            (_, Some(ref data)) => data.len() as libc::c_ulong,
            (_, None) => 0,
        }).collect();
        let is_nulls = input.iter()
            .map(|&(_, ref data)| if data.is_none() { 1 } else { 0 })
            .collect();
        let (types, data) = input.into_iter().unzip::<_, _, Vec<_>, _>();

        let mysql_binds = types.into_iter().map(|tpe| {
            let mut bind: ffi::MYSQL_BIND = unsafe { mem::zeroed() };
            bind.buffer_type = mysql_type_to_ffi_type(tpe);
            bind
        }).collect();

        let mut res = Binds {
            data: data,
            lengths: lengths,
            is_nulls: is_nulls,
            mysql_binds: mysql_binds,
        };
        unsafe { res.link_mysql_bind_pointers(); }
        res
    }

    pub fn mysql_binds(&mut self) -> &mut [ffi::MYSQL_BIND] {
        &mut self.mysql_binds
    }

    // This function relies on the invariant that no further mutations to this
    // struct will occur after this function has been called.
    unsafe fn link_mysql_bind_pointers(&mut self) {
        for (i, bind_data) in self.data.iter_mut().enumerate() {
            if let Some(ref mut data) = bind_data.as_mut() {
                self.mysql_binds[i].buffer = data.as_mut_ptr() as *mut libc::c_void;
                self.mysql_binds[i].buffer_length = data.capacity() as libc::c_ulong;
            }
            self.mysql_binds[i].length = &mut self.lengths[i];
            self.mysql_binds[i].is_null = &mut self.is_nulls[i];
        }
    }
}

fn mysql_type_to_ffi_type(tpe: MysqlType) -> ffi::enum_field_types {
    match tpe {
        MysqlType::Tiny => ffi::enum_field_types::MYSQL_TYPE_TINY,
        MysqlType::Short => ffi::enum_field_types::MYSQL_TYPE_SHORT,
        MysqlType::Long => ffi::enum_field_types::MYSQL_TYPE_LONG,
        MysqlType::LongLong => ffi::enum_field_types::MYSQL_TYPE_LONGLONG,
        MysqlType::Float => ffi::enum_field_types::MYSQL_TYPE_FLOAT,
        MysqlType::Double => ffi::enum_field_types::MYSQL_TYPE_DOUBLE,
        MysqlType::Time => ffi::enum_field_types::MYSQL_TYPE_TIME,
        MysqlType::Date => ffi::enum_field_types::MYSQL_TYPE_DATE,
        MysqlType::DateTime => ffi::enum_field_types::MYSQL_TYPE_DATETIME,
        MysqlType::Timestamp => ffi::enum_field_types::MYSQL_TYPE_TIMESTAMP,
        MysqlType::String => ffi::enum_field_types::MYSQL_TYPE_STRING,
        MysqlType::Blob => ffi::enum_field_types::MYSQL_TYPE_BLOB,
    }
}
