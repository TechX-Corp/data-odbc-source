use crate::{
    binding::*, chkerr, connection::Connection, error::Error, oracle_type::OracleType, to_odpi_str,
    to_rust_slice, to_rust_str, Context,
};
use std::{fmt, mem::MaybeUninit, os::raw::c_char, ptr, slice, sync::Arc};

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    name: String,
    oracle_type: OracleType,
    nullable: bool,
}

impl ColumnInfo {
    fn new(stmt: &Statement, idx: usize) -> Result<ColumnInfo, Error> {
        let mut info = MaybeUninit::uninit();
        chkerr!(
            stmt.ctxt(),
            dpiStmt_getQueryInfo(stmt.handle(), (idx + 1) as u32, info.as_mut_ptr())
        );
        let info = unsafe { info.assume_init() };
        Ok(ColumnInfo {
            name: to_rust_str(info.name, info.nameLength),
            oracle_type: OracleType::from_type_info(&info.typeInfo)?,
            nullable: info.nullOk != 0,
        })
    }

    /// Gets column name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets Oracle type
    pub fn oracle_type(&self) -> &OracleType {
        &self.oracle_type
    }

    /// Gets whether the column may be NULL.
    /// False when the column is defined as `NOT NULL`.
    pub fn nullable(&self) -> bool {
        self.nullable
    }
}

impl fmt::Display for ColumnInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.nullable {
            write!(f, "{} {}", self.name, self.oracle_type)
        } else {
            write!(f, "{} {} NOT NULL", self.name, self.oracle_type)
        }
    }
}

/// Statement
// #[derive(Debug)]
pub struct Statement<'conn> {
    pub(crate) handle: *mut dpiStmt,
    pub(crate) conn: &'conn Connection,
    tag: String,
    column_info: Vec<ColumnInfo>,
    sql_values: Vec<Arc<SqlValue<'conn>>>,
    fetch_array_size: u32,
}

impl<'conn> Statement<'conn> {
    pub fn new(conn: &'conn Connection, sql: &str, fetch_array_size: u32) -> Result<Self, Error> {
        let sql = to_odpi_str(sql);
        let tag = to_odpi_str("");
        let mut handle: *mut dpiStmt = ptr::null_mut();
        chkerr!(
            conn.ctxt(),
            dpiConn_prepareStmt(
                conn.handle(),
                0,
                sql.ptr,
                sql.len,
                tag.ptr,
                tag.len,
                &mut handle
            )
        );
        Ok(Self {
            handle,
            conn,
            tag: "".into(),
            column_info: Vec::new(),
            sql_values: Vec::new(),
            fetch_array_size,
        })
    }

    fn ctxt(&self) -> &'static Context {
        self.conn.ctxt
    }

    fn handle(&self) -> *mut dpiStmt {
        self.handle
    }

    pub fn execute(&mut self) -> Result<(), Error> {
        let mut num_query_columns = 0;
        chkerr!(
            self.ctxt(),
            dpiStmt_setFetchArraySize(self.handle(), self.fetch_array_size)
        );
        chkerr!(
            self.ctxt(),
            dpiStmt_execute(self.handle(), DPI_MODE_EXEC_DEFAULT, &mut num_query_columns)
        );
        self.column_info = Vec::with_capacity(num_query_columns as usize);
        self.sql_values = Vec::with_capacity(num_query_columns as usize);
        for i in 0..num_query_columns {
            let ci = ColumnInfo::new(self, i as usize)?;
            let sv = SqlValue::new(self.conn, &ci.oracle_type, self.fetch_array_size)?;
            self.column_info.push(ci);
            chkerr!(
                self.ctxt(),
                dpiStmt_define(self.handle(), i + 1, sv.handle())
            );
            self.sql_values.push(Arc::new(sv));
        }

        Ok(())
    }

    pub fn fetch_next_batch(&self) -> Result<(u32, u32, bool), Error> {
        let mut buffer_row_index = 0;
        let mut num_row_fetched = 0;
        let mut more_rows = 0;
        chkerr!(
            self.ctxt(),
            dpiStmt_fetchRows(
                self.handle(),
                self.fetch_array_size,
                &mut buffer_row_index,
                &mut num_row_fetched,
                &mut more_rows
            )
        );
        Ok((buffer_row_index, num_row_fetched, more_rows == 1))
    }

    pub fn sql_values(&self) -> Vec<Arc<SqlValue>> {
        self.sql_values.clone()
    }

    pub fn column_info(&self) -> Vec<ColumnInfo> {
        self.column_info.clone()
    }

    fn close(&mut self) -> Result<(), Error> {
        let tag = to_odpi_str(&self.tag);
        chkerr!(self.ctxt(), dpiStmt_close(self.handle, tag.ptr, tag.len));
        Ok(())
    }
}

impl Drop for Statement<'_> {
    fn drop(&mut self) {
        let _ = self.close();
        // println!("Release STMT");
        unsafe { dpiStmt_release(self.handle) };
    }
}

pub struct SqlValue<'conn> {
    conn: &'conn Connection,
    pub(crate) handle: *mut dpiVar,
    data: *mut dpiData,
    // native_type: NativeType,
    oratype: OracleType,
    // pub(crate) array_size: u32,
    // pub(crate) buffer_row_index: BufferRowIndex,
    // keep_bytes: Vec<u8>,
    // keep_dpiobj: *mut dpiObject,
    // pub(crate) lob_bind_type: LobBindType,
    // pub(crate) query_params: QueryParams,
}

impl<'conn> SqlValue<'conn> {
    fn new(conn: &'conn Connection, oratype: &OracleType, array_size: u32) -> Result<Self, Error> {
        let oratype = match oratype {
            OracleType::CLOB => &OracleType::Long,
            OracleType::NCLOB => {
                // When the size is larger than DPI_MAX_BASIC_BUFFER_SIZE, ODPI-C uses
                // a dynamic buffer instead of a fixed-size buffer.
                &OracleType::NVarchar2(DPI_MAX_BASIC_BUFFER_SIZE + 1)
            }
            OracleType::BLOB => &OracleType::LongRaw,
            OracleType::BFILE => &OracleType::LongRaw,
            _ => oratype,
        }
        .clone();
        let (oratype_num, native_type, size, size_is_byte) = oratype.var_create_param()?;
        let native_type_num = native_type.to_native_type_num();
        let mut handle: *mut dpiVar = ptr::null_mut();
        let mut data: *mut dpiData = ptr::null_mut();
        chkerr!(
            conn.ctxt(),
            dpiConn_newVar(
                conn.handle(),
                oratype_num,
                native_type_num,
                array_size,
                size,
                size_is_byte,
                0,
                ptr::null_mut(),
                &mut handle,
                &mut data
            )
        );
        Ok(Self {
            conn,
            handle,
            data,
            oratype,
            // array_size,
        })
    }

    pub(crate) fn handle(&self) -> *mut dpiVar {
        self.handle
    }

    fn data(&self, offset: isize) -> *mut dpiData {
        unsafe { self.data.offset(offset) }
    }

    /// Fetch string from buffer
    ///
    pub fn get_string(&self, offset: isize) -> Result<Option<String>, Error> {
        match &self.oratype {
            OracleType::CLOB => self.get_clob_as_string_unchecked(offset),
            _ => Ok(self.get_string_unchecked(offset)),
        }
    }

    /// Fetch string column from normal column
    fn get_string_unchecked(&self, offset: isize) -> Option<String> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            unsafe {
                let bytes = dpiData_getBytes(data);
                Some(to_rust_str((*bytes).ptr, (*bytes).length))
            }
        }
    }

    /// Fetch number as decimal values
    pub fn get_i128(&self, offset: isize, scale: u32) -> Option<i128> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            let bytes = unsafe { dpiData_getBytes(data) };
            let s =
                unsafe { slice::from_raw_parts((*bytes).ptr as *mut u8, (*bytes).length as usize) };
            // Unsafe, assume successful
            let s = std::str::from_utf8(s).unwrap();
            let mut split = s.split('.');
            match split.next() {
                None => None,
                Some(first) => {
                    // println!("{:?} {:?}", s, first);
                    let sign = if first.starts_with('-') { -1 } else { 1 };
                    let first = first.parse::<i128>().ok().map(|v| v * 10i128.pow(scale));
                    match split.next() {
                        None => first,
                        Some(second) => {
                            let left_over = scale - second.len() as u32;
                            let second = second.parse::<i128>().ok();
                            match (first, second) {
                                (Some(first), Some(second)) => {
                                    Some(first + second * 10i128.pow(left_over) * sign)
                                }
                                _ => None,
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn get_f64_from_bytes(&self, offset: isize) -> Option<f64> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            let bytes = unsafe { dpiData_getBytes(data) };
            let s =
                unsafe { slice::from_raw_parts((*bytes).ptr as *mut u8, (*bytes).length as usize) };
            // Unsafe, assume successful
            let s = std::str::from_utf8(s).unwrap();
            s.parse::<f64>().ok()
        }
    }

    pub fn get_timestamp(&self, offset: isize) -> Option<i64> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            unsafe {
                let timestamp = dpiData_getTimestamp(data);
                chrono::NaiveDate::from_ymd_opt(
                    (*timestamp).year as i32,
                    (*timestamp).month as u32,
                    (*timestamp).day as u32,
                )
                .and_then(|date| {
                    date.and_hms_milli_opt(
                        (*timestamp).hour as u32,
                        (*timestamp).minute as u32,
                        (*timestamp).second as u32,
                        (*timestamp).fsecond / 1_000_000,
                    )
                })
                .map(|ts| ts.and_utc().timestamp_millis())
            }
        }
    }

    pub fn get_f32(&self, offset: isize) -> Option<f32> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            unsafe { Some(dpiData_getFloat(data)) }
        }
    }

    pub fn get_f64(&self, offset: isize) -> Option<f64> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        if is_null {
            None
        } else {
            unsafe { Some(dpiData_getDouble(data)) }
        }
    }

    fn get_clob_as_string_unchecked(&self, offset: isize) -> Result<Option<String>, Error> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        const READ_CHAR_SIZE: u64 = 8192;
        if is_null {
            Ok(None)
        } else {
            let lob = unsafe { dpiData_getLOB(data) };
            let mut total_char_size = 0;
            let mut total_byte_size = 0;
            let mut bufsiz = 0;
            unsafe {
                dpiLob_getSize(lob, &mut total_char_size);
                dpiLob_getBufferSize(lob, total_char_size, &mut total_byte_size);
                dpiLob_getBufferSize(lob, READ_CHAR_SIZE, &mut bufsiz);
            }
            let mut result = String::with_capacity(total_byte_size as usize);
            let mut buf = vec![0u8; bufsiz as usize];
            let bufptr = buf.as_mut_ptr() as *mut c_char;
            let mut offset = 1;
            while offset <= total_char_size {
                let mut read_len = bufsiz;
                chkerr!(
                    self.conn.ctxt(),
                    dpiLob_readBytes(lob, offset, READ_CHAR_SIZE, bufptr, &mut read_len)
                );
                result.push_str(std::str::from_utf8(&buf[..(read_len as usize)])?);
                offset += READ_CHAR_SIZE;
            }
            Ok(Some(result))
        }
    }

    pub fn get_blob(&self, offset: isize) -> Result<Option<Vec<u8>>, Error> {
        match &self.oratype {
            OracleType::BLOB => self.get_blob_unchecked(offset),
            _ => self.get_blob_from_raw_unchecked(offset),
        }
    }

    fn get_blob_from_raw_unchecked(&self, offset: isize) -> Result<Option<Vec<u8>>, Error> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        match is_null {
            true => Ok(None),
            false => unsafe {
                let bytes = dpiData_getBytes(data);
                let mut vec = Vec::with_capacity((*bytes).length as usize);
                vec.extend_from_slice(to_rust_slice((*bytes).ptr, (*bytes).length));
                Ok(Some(vec))
            },
        }
    }

    fn get_blob_unchecked(&self, offset: isize) -> Result<Option<Vec<u8>>, Error> {
        let data = self.data(offset);
        let is_null = unsafe { (*data).isNull != 0 };
        match is_null {
            true => Ok(None),
            false => {
                let lob = unsafe { dpiData_getLOB(data) };
                let mut total_size = 0;
                unsafe {
                    dpiLob_getSize(lob, &mut total_size);
                }
                println!("{total_size:?}");
                let mut result: Vec<u8> = Vec::with_capacity(total_size as usize);
                let mut read_len = total_size;
                chkerr!(
                    self.conn.ctxt(),
                    dpiLob_readBytes(
                        lob,
                        1,
                        total_size,
                        result.as_mut_ptr() as *mut c_char,
                        &mut read_len
                    )
                );
                unsafe {
                    result.set_len(read_len as usize);
                }
                Ok(Some(result))
            }
        }
    }
}

unsafe impl Send for SqlValue<'_> {}
unsafe impl Sync for SqlValue<'_> {}

impl Drop for SqlValue<'_> {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { dpiVar_release(self.handle) };
        }
        // if !self.keep_dpiobj.is_null() {
        //     unsafe { dpiObject_release(self.keep_dpiobj) };
        // }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

    fn internal_test_query() -> Result<(), Error> {
        let conn = Connection::connect(
            "tx",
            "123123",
            "localhost:1521/pdb1",
        )?;
        let mut stmt = Statement::new(
            &conn,
            "SELECT ID, NUMBER_3, TIMESTAMP_COL FROM TEST_ALL_TYPE_1 WHERE ROWNUM<=100 ORDER BY ID",
            100,
        )?;
        stmt.execute()?;
        stmt.fetch_next_batch()?;
        let column_info = stmt.column_info();
        let rvs = stmt.sql_values();

        thread::scope(|sc| {
            let mut join_handlers = Vec::with_capacity(column_info.len());
            for col_idx in 0..column_info.len() {
                let ci = (&column_info[col_idx]).clone();
                let rv = rvs[col_idx].clone();
                let jh = sc.spawn(move || match ci.oracle_type() {
                    OracleType::Number(_, scale) => {
                        for offset in 0..100 {
                            println!("{:?} {:?}", ci.name(), rv.get_i128(offset, *scale as u32))
                        }
                    }
                    OracleType::Timestamp(_)
                    | OracleType::TimestampLTZ(_)
                    | OracleType::TimestampTZ(_) => {
                        for offset in 0..100 {
                            println!("{:?} {:?}", ci.name(), rv.get_timestamp(offset))
                        }
                    }
                    _ => {
                        for offset in 0..100 {
                            println!("{:?} {:?}", ci.name(), rv.get_string(offset))
                        }
                    }
                });
                join_handlers.push(jh);
            }
            join_handlers
                .into_iter()
                .map(|jh| jh.join())
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|_| Error::InternalError("Join error".to_string()))?;
        Ok(())
    }

    #[test]
    fn test_query() {
        let rs = internal_test_query();
        println!("{:?}", rs)
    }

    fn query_2() -> Result<(), Error> {
        let conn = Connection::connect(
            "tx",
            "123123",
            "localhost:1521/pdb1",
        )?;
        let mut stmt = Statement::new(
            &conn,
            "SELECT ID, NUMBER_3, TIMESTAMP_COL FROM TEST_ALL_TYPE_1 WHERE ROWNUM<=1000000",
            5000,
        )?;
        stmt.execute()?;
        println!("{:?}", stmt.column_info());
        let mut more_rows = true;
        let mut nrows = 0;
        let mut total_rows = nrows;
        while more_rows {
            (_, nrows, more_rows) = stmt.fetch_next_batch()?;
            total_rows += nrows;
        }
        println!("nrows: {:?}", total_rows);
        Ok(())
    }

    #[test]
    fn test_query_2() {
        let rs = query_2();
        println!("{rs:?}");
    }
}
