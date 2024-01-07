use anyhow::{bail, Result};
use std::fmt;

pub struct Tokenizer<'a> {
    input: &'a str,
    index: usize,
}

impl Tokenizer<'_> {
    pub fn new<'a>(input: &'a str) -> Tokenizer {
        Tokenizer { input, index: 0 }
    }

    fn get_char(&self, index: usize) -> (Option<char>, usize) {
        let c = self.input.chars().nth(index);
        (c, index + 1)
    }

    pub fn take_while(&mut self, func: impl Fn(&Token) -> bool) -> Vec<Token> {
        let mut result = Vec::new();
        let mut index = self.index;

        while let (Some(t), i) = self.get_token(index) {
            if !func(&t) {
                break;
            }

            index = i;
            result.push(t);
        }

        self.index = index;
        result
    }

    fn get_token(&self, index: usize) -> (Option<Token>, usize) {
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
                let result = &self.input[index..index_iter];
                let number = result.parse::<i64>().unwrap();
                (Some(Token::Number(number)), index_iter)
            }
            c if c.is_alphabetic() => {
                while let (Some(c), i) = self.get_char(index_iter) {
                    if !c.is_alphanumeric() && c != '_' {
                        break;
                    }

                    index_iter = i;
                }
                let result = &self.input[index..index_iter];
                (Some(Token::Text(String::from(result))), index_iter)
            }
            c if c == '\'' || c == '"' => {
                while let (Some(ch), i) = self.get_char(index_iter) {
                    if ch == c {
                        if let (Some(ch), _) = self.get_char(i) {
                            if ch == c {
                                index_iter = i + 1;
                                continue;
                            }
                        }

                        index_iter = i;
                        break;
                    }

                    index_iter = i;
                }

                let result = &self.input[index + 1..index_iter - 1];
                return (Some(Token::String(String::from(result))), index_iter);
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
            _ => (Some(Token::Punctuation(c)), index_iter),
        }
    }

    pub fn remaining(&self) -> &str {
        &self.input[self.index..]
    }

    pub fn peek(&mut self) -> Option<Token> {
        let (token, _) = self.get_token(self.index);
        token
    }

    pub fn next(&mut self) -> Option<Token> {
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
        let tag = tag.to_lowercase();

        match token {
            Token::Text(t) => {
                if t.to_lowercase() != tag {
                    bail!("Expected token: '{}', found: '{}'", tag, t);
                }
            }
            Token::Punctuation(c) => {
                if tag.len() != 1 {
                    bail!("Expected token: '{}', found: '{}'", tag, c);
                }

                if c != tag.chars().next().unwrap() {
                    bail!("Expected token: '{}', found: '{}'", tag, c);
                }
            }
            _ => bail!("Expected token: '{}', found: '{}'", tag, token),
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Number(i64),
    String(String),
    Text(String),
    Punctuation(char),
}

impl From<Token> for String {
    fn from(t: Token) -> Self {
        match t {
            Token::Number(n) => n.to_string(),
            Token::String(s) => s,
            Token::Text(t) => t,
            Token::Punctuation(c) => String::from(c),
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Number(n) => write!(f, "{}", n),
            Token::String(s) => write!(f, "{}", s),
            Token::Text(t) => write!(f, "{}", t),
            Token::Punctuation(c) => write!(f, "{}", c),
        }
    }
}
