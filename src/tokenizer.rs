use anyhow::{bail, Result};

pub struct Tokenizer<'a> {
    input: &'a str,
    index: usize,
}

impl<'a> Tokenizer<'a> {
    pub fn new(input: &'a str) -> Tokenizer<'a> {
        Tokenizer { input, index: 0 }
    }

    fn get_char(&self, index: usize) -> (Option<char>, usize) {
        let c = self.input.chars().nth(index);
        (c, index + 1)
    }

    pub fn next_char(&mut self) -> Option<char> {
        let (c, i) = self.get_char(self.index);
        self.index = i;
        c
    }

    pub fn take_while(&mut self, func: impl Fn(&str) -> bool) -> Vec<&'a str> {
        let mut result = Vec::new();
        let mut index = self.index;
        while let (Some(t), i) = self.get_token(index) {
            if !func(t) {
                break;
            }

            index = i;
            result.push(t);
        }

        self.index = index;
        result
    }

    fn get_token(&mut self, index: usize) -> (Option<&'a str>, usize) {
        let (c, i) = self.get_char(index);

        if c.is_none() {
            return (None, i);
        }

        let mut index_iter = i;
        let c = c.unwrap();
        match c {
            c if c.is_numeric() => {
                while let (Some(c), i) = self.get_char(index_iter) {
                    if !c.is_numeric() {
                        break;
                    }

                    index_iter = i;
                }
            }
            c if c.is_alphabetic() => {
                while let (Some(c), i) = self.get_char(index_iter) {
                    if !c.is_alphanumeric() && c != '_' {
                        break;
                    }

                    index_iter = i;
                }
            }
            ch if " \n\t".contains(c) => {
                while let (Some(c), i) = self.get_char(index_iter) {
                    if c != ch {
                        break;
                    }

                    index_iter = i;
                }

                return self.get_token(index_iter);
            }
            _ => (),
        }

        let result = Some(&self.input[index..index_iter]);
        (result, index_iter)
    }

    pub fn next(&mut self) -> Option<&'a str> {
        let (token, index) = self.get_token(self.index);
        self.index = index;
        token
    }

    pub fn tag(&mut self, tag: &str) -> Result<()> {
        let token = self.next();

        if token.is_none() {
            bail!("No tokens left in input");
        }

        let token = token.unwrap();

        if token != tag {
            bail!("Expected token: {}, found: {}", tag, token);
        }

        Ok(())
    }
}
