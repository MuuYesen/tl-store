//! Private OAuth files on Windows: owned protected DACL, no reparse fallback.
//! https://learn.microsoft.com/windows/win32/api/aclapi/nf-aclapi-getsecurityinfo
//! https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-createfilew
use anyhow::{Context, Result, ensure};
use std::os::windows::{
    ffi::OsStrExt,
    io::{AsRawHandle, FromRawHandle, OwnedHandle},
};
use std::{ffi::c_void, fs::File, mem::size_of, path::Path, ptr};
use windows_sys::Win32::{
    Foundation::{ERROR_ALREADY_EXISTS, GetLastError, INVALID_HANDLE_VALUE, LocalFree},
    Security::{Authorization::*, *},
    Storage::FileSystem::*,
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

struct Local(*mut c_void);
impl Drop for Local {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe {
                LocalFree(self.0);
            }
        }
    }
}

struct Sid(Vec<u32>);
impl Sid {
    fn pointer(&self) -> PSID {
        self.0.as_ptr().cast_mut().cast()
    }
    fn current() -> Result<Self> {
        let mut handle = ptr::null_mut();
        ensure!(
            unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut handle) } != 0,
            "Cannot inspect OAuth file owner token"
        );
        let token = unsafe { OwnedHandle::from_raw_handle(handle) };
        let mut bytes = 0;
        unsafe {
            GetTokenInformation(
                token.as_raw_handle(),
                TokenUser,
                ptr::null_mut(),
                0,
                &raw mut bytes,
            );
        }
        ensure!(
            (1..=65536).contains(&bytes),
            "Invalid OAuth owner token length"
        );
        let mut data = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
        ensure!(
            unsafe {
                GetTokenInformation(
                    token.as_raw_handle(),
                    TokenUser,
                    data.as_mut_ptr().cast(),
                    bytes,
                    &raw mut bytes,
                )
            } != 0,
            "Cannot read OAuth file owner token"
        );
        let sid = unsafe { (*data.as_ptr().cast::<TOKEN_USER>()).User.Sid };
        ensure!(unsafe { IsValidSid(sid) } != 0, "Invalid OAuth owner SID");
        let length = unsafe { GetLengthSid(sid) };
        ensure!((1..=68).contains(&length), "Invalid OAuth owner SID length");
        let result = Self(vec![0u32; (length as usize).div_ceil(4)]);
        ensure!(
            unsafe { CopySid(length, result.pointer(), sid) } != 0,
            "Cannot copy OAuth owner identity"
        );
        Ok(result)
    }
    fn well_known(kind: WELL_KNOWN_SID_TYPE) -> Result<Self> {
        let result = Self(vec![0u32; 17]);
        let mut bytes = 68;
        ensure!(
            unsafe { CreateWellKnownSid(kind, ptr::null_mut(), result.pointer(), &raw mut bytes) }
                != 0,
            "Cannot inspect trusted Windows principal"
        );
        Ok(result)
    }
    fn text(&self) -> Result<String> {
        let mut raw = ptr::null_mut();
        ensure!(
            unsafe { ConvertSidToStringSidW(self.pointer(), &raw mut raw) } != 0,
            "Cannot encode OAuth owner identity"
        );
        let allocation = Local(raw.cast());
        let mut length = 0;
        while length < 256 && unsafe { *raw.add(length) } != 0 {
            length += 1;
        }
        ensure!(length < 256, "Invalid OAuth owner identity length");
        let text = String::from_utf16(unsafe { std::slice::from_raw_parts(raw, length) })
            .context("Invalid OAuth owner identity encoding")?;
        drop(allocation);
        Ok(text)
    }
}

fn wide(path: &Path) -> Result<Vec<u16>> {
    let mut value: Vec<u16> = path.as_os_str().encode_wide().collect();
    ensure!(!value.contains(&0), "NUL in OAuth path");
    value.push(0);
    Ok(value)
}

fn descriptor() -> Result<Local> {
    let user = Sid::current()?.text()?;
    // Protected, inheritable only by this user, SYSTEM and administrators.
    let sddl = format!("O:{user}D:P(A;OICI;FA;;;{user})(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)");
    let text: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut pointer = ptr::null_mut();
    ensure!(
        unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                text.as_ptr(),
                1,
                &raw mut pointer,
                ptr::null_mut(),
            )
        } != 0,
        "Cannot create private OAuth security descriptor"
    );
    Ok(Local(pointer))
}

fn attributes(descriptor: &Local) -> SECURITY_ATTRIBUTES {
    SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    }
}

fn validate(file: &File, directory: bool) -> Result<()> {
    let handle = file.as_raw_handle();
    let mut tag: FILE_ATTRIBUTE_TAG_INFO = unsafe { std::mem::zeroed() };
    ensure!(
        unsafe {
            GetFileInformationByHandleEx(
                handle,
                FileAttributeTagInfo,
                (&raw mut tag).cast(),
                size_of::<FILE_ATTRIBUTE_TAG_INFO>() as u32,
            )
        } != 0,
        "Cannot inspect OAuth file identity"
    );
    ensure!(
        tag.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT == 0,
        "OAuth reparse points are forbidden"
    );
    let metadata = file
        .metadata()
        .context("Cannot inspect private OAuth object")?;
    ensure!(
        if directory {
            metadata.is_dir()
        } else {
            metadata.is_file()
        },
        "Wrong OAuth object type"
    );
    let mut owner = ptr::null_mut();
    let mut dacl = ptr::null_mut();
    let mut raw = ptr::null_mut();
    let code = unsafe {
        GetSecurityInfo(
            handle,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &raw mut owner,
            ptr::null_mut(),
            &raw mut dacl,
            ptr::null_mut(),
            &raw mut raw,
        )
    };
    let _allocation = Local(raw);
    ensure!(
        code == 0 && !raw.is_null() && !owner.is_null() && !dacl.is_null(),
        "Cannot inspect private OAuth DACL"
    );
    let user = Sid::current()?;
    ensure!(
        unsafe { EqualSid(owner, user.pointer()) } != 0,
        "OAuth object is not owned by this user"
    );
    let mut control = 0;
    let mut revision = 0;
    ensure!(
        unsafe { GetSecurityDescriptorControl(raw, &raw mut control, &raw mut revision) } != 0
            && control & SE_DACL_PROTECTED != 0,
        "OAuth DACL must be protected"
    );
    let system = Sid::well_known(WinLocalSystemSid)?;
    let admins = Sid::well_known(WinBuiltinAdministratorsSid)?;
    for index in 0..unsafe { (*dacl).AceCount } {
        let mut ace = ptr::null_mut();
        ensure!(
            unsafe { GetAce(dacl, u32::from(index), &raw mut ace) } != 0 && !ace.is_null(),
            "Invalid OAuth DACL entry"
        );
        let header = unsafe { &*ace.cast::<ACE_HEADER>() };
        match header.AceType {
            0 => {
                // ACCESS_ALLOWED_ACE_TYPE; other grant layouts are not silently accepted.
                ensure!(
                    header.AceSize as usize >= size_of::<ACCESS_ALLOWED_ACE>(),
                    "Invalid OAuth grant entry"
                );
                let allowed = unsafe { &*ace.cast::<ACCESS_ALLOWED_ACE>() };
                let sid = (&raw const allowed.SidStart).cast_mut().cast();
                ensure!(
                    unsafe { IsValidSid(sid) } != 0,
                    "Invalid OAuth grant principal"
                );
                ensure!(
                    [user.pointer(), system.pointer(), admins.pointer()]
                        .iter()
                        .any(|trusted| unsafe { EqualSid(sid, *trusted) } != 0),
                    "OAuth object grants another principal access"
                );
            }
            1 => {} // ACCESS_DENIED_ACE_TYPE cannot broaden access.
            _ => anyhow::bail!("Unsupported OAuth DACL grant layout"),
        }
    }
    Ok(())
}

pub(crate) fn check_directory(path: &Path, create: bool) -> Result<()> {
    if create {
        let descriptor = descriptor()?;
        let attributes = attributes(&descriptor);
        let mut parents: Vec<_> = path.ancestors().collect();
        parents.reverse();
        for parent in parents {
            if parent.is_dir() {
                continue;
            }
            let name = wide(parent)?;
            let made = unsafe { CreateDirectoryW(name.as_ptr(), &raw const attributes) };
            ensure!(
                made != 0 || unsafe { GetLastError() } == ERROR_ALREADY_EXISTS,
                "Cannot create private OAuth directory"
            );
        }
    }
    let name = wide(path)?;
    let raw = unsafe {
        CreateFileW(
            name.as_ptr(),
            0x80020000,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            ptr::null_mut(),
        )
    };
    ensure!(
        raw != INVALID_HANDLE_VALUE,
        "Private OAuth directory unavailable"
    );
    let file = unsafe { File::from_raw_handle(raw) };
    validate(&file, true)
}

pub(crate) fn open_file(path: &Path, create: bool, exclusive_new: bool) -> Result<File> {
    let descriptor = descriptor()?;
    let attributes = attributes(&descriptor);
    let name = wide(path)?;
    let disposition = if exclusive_new {
        CREATE_NEW
    } else if create {
        OPEN_ALWAYS
    } else {
        OPEN_EXISTING
    };
    let access = 0x80020000 | if create { 0x40000000 } else { 0 };
    let raw = unsafe {
        CreateFileW(
            name.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &raw const attributes,
            disposition,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_WRITE_THROUGH,
            ptr::null_mut(),
        )
    };
    ensure!(
        raw != INVALID_HANDLE_VALUE,
        "Private OAuth file unavailable"
    );
    let file = unsafe { File::from_raw_handle(raw) };
    validate(&file, false)?;
    Ok(file)
}

pub(crate) fn commit(source: &Path, destination: &Path) -> Result<()> {
    ensure!(
        source.parent() == destination.parent(),
        "OAuth commit must stay in one directory"
    );
    let source = wide(source)?;
    let destination = wide(destination)?;
    ensure!(
        unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        } != 0,
        "Cannot atomically commit OAuth state"
    );
    Ok(())
}
