use crate::error::Error;
use crate::DpiConn;
use crate::{binding::*, chkerr};
use crate::{to_odpi_str, Context};
use std::ptr;

pub struct Connection {
    pub(crate) ctxt: &'static Context,
    handle: DpiConn,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    pub fn connect(username: &str, password: &str, connect_string: &str) -> Result<Self, Error> {
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
        Ok(Self {
            ctxt,
            handle: DpiConn::new(handle),
        })
    }

    pub(crate) fn ctxt(&self) -> &'static Context {
        self.ctxt
    }

    pub(crate) fn handle(&self) -> *mut dpiConn {
        self.handle.raw()
    }
}
