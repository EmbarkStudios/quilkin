#[derive(thiserror::Error, Debug)]
pub enum ObjError {
    #[error(transparent)]
    Parse(#[from] object::read::Error),
    #[error("global {0} not found but was required")]
    GlobalNotFound(String),
    #[error("the object has no symbol table")]
    NoSymbolTable,
}

pub struct Loader<'f> {
    obj: object::read::File<'f>,
}

pub struct Global<'s> {
    pub name: &'s str,
    pub value: &'s [u8],
    pub must_exist: bool,
    pub set: bool,
}

impl<'f> Loader<'f> {
    pub fn parse(data: &'f [u8]) -> Result<Self, ObjError> {
        let obj = object::read::File::parse(data)?;

        Ok(Self { obj })
    }

    pub fn patch_globals(&mut self, globals: &mut [Global<'_>]) -> Result<(), ObjError> {
        use object::{Object, ObjectSymbol, read::ObjectSymbolTable};

        for g in globals {
            g.set = false;
        }

        let mut to_set = globals.len();

        let sym_tab = self.obj.symbol_table().ok_or(ObjError::NoSymbolTable)?;

        for sym in sym_tab.symbols() {
            let Ok(sym_name) = sym.name() else {
                continue;
            };
            let Some(global) = globals.iter().find(|g| g.name == sym_name) else {
                continue;
            };
        }

        Ok(())
    }
}
