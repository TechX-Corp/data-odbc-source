use crate::binding::*;
pub use crate::connection::Connection;
pub use crate::error::Error;
pub use crate::oracle_type::OracleType;
pub use crate::statement::{ColumnInfo, SqlValue, Statement};
use once_cell::sync::Lazy;
use std::{mem::MaybeUninit, os::raw::c_char, ptr, slice};

#[allow(dead_code)]
#[allow(non_camel_case_types)]
#[allow(non_snake_case)]
#[allow(improper_ctypes)]
mod binding;
mod connection;
mod error;
mod oracle_type;
mod statement;

struct Context {
    pub context: *mut dpiContext,
}

enum ContextResult {
    Ok(Context),
    Err(dpiErrorInfo),
}

unsafe impl Sync for ContextResult {}
unsafe impl Send for ContextResult {}

static DPI_CONTEXT: Lazy<ContextResult> = Lazy::new(|| {
    let mut ctxt = ptr::null_mut();
    let mut err = MaybeUninit::uninit();
    if unsafe {
        dpiContext_createWithParams(
            DPI_MAJOR_VERSION,
            DPI_MINOR_VERSION,
            ptr::null_mut(),
            &mut ctxt,
            err.as_mut_ptr(),
        )
    } == DPI_SUCCESS as i32
    {
        ContextResult::Ok(Context { context: ctxt })
    } else {
        ContextResult::Err(unsafe { err.assume_init() })
    }
});

impl Context {
    pub fn get() -> Result<&'static Context, Error> {
        match *DPI_CONTEXT {
            ContextResult::Ok(ref ctxt) => Ok(ctxt),
            ContextResult::Err(ref err) => Err(error::error_from_dpi_error(err)),
        }
    }

    pub fn common_create_params(&self) -> dpiCommonCreateParams {
        let mut params = MaybeUninit::uninit();
        unsafe {
            dpiContext_initCommonCreateParams(self.context, params.as_mut_ptr());
            let mut params = params.assume_init();
            let driver_name: &'static str = concat!("rust-oracle : ", env!("CARGO_PKG_VERSION"));
            params.createMode |= DPI_MODE_CREATE_THREADED;
            params.driverName = driver_name.as_ptr() as *const c_char;
            params.driverNameLength = driver_name.len() as u32;
            params
        }
    }

    pub fn conn_create_params(&self) -> dpiConnCreateParams {
        let mut params = MaybeUninit::uninit();
        unsafe {
            dpiContext_initConnCreateParams(self.context, params.as_mut_ptr());
            params.assume_init()
        }
    }

    // pub fn pool_create_params(&self) -> dpiPoolCreateParams {
    //     let mut params = MaybeUninit::uninit();
    //     unsafe {
    //         dpiContext_initPoolCreateParams(self.context, params.as_mut_ptr());
    //         params.assume_init()
    //     }
    // }
}

unsafe impl Sync for Context {}
unsafe impl Send for Context {}

fn to_rust_str(ptr: *const c_char, len: u32) -> String {
    if ptr.is_null() {
        "".to_string()
    } else {
        let s = unsafe { slice::from_raw_parts(ptr as *mut u8, len as usize) };
        String::from_utf8_lossy(s).into_owned()
    }
}

fn to_rust_slice<'a>(ptr: *const c_char, len: u32) -> &'a [u8] {
    if ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(ptr as *mut u8, len as usize) }
    }
}

struct OdpiStr {
    pub ptr: *const c_char,
    pub len: u32,
}

fn to_odpi_str(s: &str) -> OdpiStr {
    if s.is_empty() {
        OdpiStr {
            ptr: ptr::null(),
            len: 0,
        }
    } else {
        OdpiStr {
            ptr: s.as_ptr() as *const c_char,
            len: s.len() as u32,
        }
    }
}

macro_rules! define_dpi_data_with_refcount {
    ($name:ident) => {
        paste::item! {
            struct [<Dpi $name>] {
                raw: *mut [<dpi $name>],
            }

            impl [<Dpi $name>] {
                fn new(raw: *mut [<dpi $name>]) -> [<Dpi $name>] {
                    [<Dpi $name>] { raw }
                }

                #[allow(dead_code)]
                fn with_add_ref(raw: *mut [<dpi $name>]) -> [<Dpi $name>] {
                    unsafe { [<dpi $name _addRef>](raw) };
                    [<Dpi $name>] { raw }
                }

                pub(crate) fn raw(&self) -> *mut [<dpi $name>] {
                    self.raw
                }
            }

            impl Clone for [<Dpi $name>] {
                fn clone(&self) -> [<Dpi $name>] {
                    unsafe { [<dpi $name _addRef>](self.raw()) };
                    [<Dpi $name>]::new(self.raw())
                }
            }

            impl Drop for [<Dpi $name>] {
                fn drop(&mut self) {
                    unsafe { [<dpi $name _release>](self.raw()) };
                }
            }

            unsafe impl Send for [<Dpi $name>] {}
            unsafe impl Sync for [<Dpi $name>] {}
        }
    };
}

define_dpi_data_with_refcount!(Conn);

#[cfg(test)]
mod test {
    use super::*;

    fn main() -> Result<(), Error> {
        let connect_string = "localhost:1521/pdb1";
        let username = "tx";
        let password = "123123";
        let username = to_odpi_str(username);
        let password = to_odpi_str(password);
        let connect_string = to_odpi_str(connect_string);
        let ctxt = Context::get()?;
        let mut handle = ptr::null_mut();
        let common_params = ctxt.common_create_params();
        let mut conn_params = ctxt.conn_create_params();
        chkerr!(
            ctxt,
            dpiConn_create(
                ctxt.context,
                username.ptr,
                username.len,
                password.ptr,
                password.len,
                connect_string.ptr,
                connect_string.len,
                &common_params,
                &mut conn_params,
                &mut handle
            )
        );
        Ok(())
    }

    #[test]
    fn test_main() {
        let rs = main();
        println!("{:?}", rs);
    }
}
