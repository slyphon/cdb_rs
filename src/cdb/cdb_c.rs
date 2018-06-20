
use std::path::Path;
use std::ffi::CStr;
use std::fs::File;
use std::os::raw::c_char;

#[repr(C)]
pub struct CDBHandle {

}

const R_OK: i32 = 0;
const R_ERR: i32 = -1;

#[no_mangle]
pub extern "C" fn cdb_rs_create(path: *const c_char, handle: *mut CDBHandle) -> i32 {
    assert!(!path.is_null());
    assert!(!handle.is_null());

    let cpath = unsafe { CStr::from_ptr(path) };

    let path =
        match cpath.to_str() {
            Ok(s) => Path::new(s),
            Err(e) => {
                error!("failed to convert path to string: {:?}, err: {:?}", cpath, e);
                return R_ERR;
            }
        };

    let fd =
        match File::open(path) {
            Ok(fp) => fp,
            Err(e) => {
                error!("failed to open file {:?}, {:?}", path, e);
                return R_ERR;
            }
        };

    R_OK
}
