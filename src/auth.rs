use axum::http::HeaderMap;
use std::collections::HashSet;

#[derive(Clone)]
pub struct AuthState {
    pub api_keys: HashSet<String>,
}

impl AuthState {
    pub fn new(api_keys: Vec<String>) -> Self {
        Self {
            api_keys: api_keys.into_iter().collect(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }

    fn extract_key(headers: &HeaderMap, query_key: Option<&str>) -> Option<String> {
        if let Some(v) = headers.get("X-API-Key").and_then(|v| v.to_str().ok()) {
            return Some(v.to_string());
        }
        query_key.map(|s| s.to_string())
    }

    pub fn check(&self, headers: &HeaderMap, query_key: Option<&str>) -> bool {
        if !self.is_enabled() {
            return true;
        }
        if let Some(key) = Self::extract_key(headers, query_key) {
            self.api_keys.contains(&key)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_disabled_when_no_keys() {
        let auth = AuthState::new(vec![]);
        assert!(!auth.is_enabled());
        assert!(auth.check(&HeaderMap::new(), None));
    }

    #[test]
    fn test_auth_with_valid_key() {
        let auth = AuthState::new(vec!["secret123".to_string()]);
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", "secret123".parse().unwrap());
        assert!(auth.check(&headers, None));
    }

    #[test]
    fn test_auth_with_invalid_key() {
        let auth = AuthState::new(vec!["secret123".to_string()]);
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", "wrong".parse().unwrap());
        assert!(!auth.check(&headers, None));
    }

    #[test]
    fn test_auth_no_key_when_required() {
        let auth = AuthState::new(vec!["secret123".to_string()]);
        assert!(!auth.check(&HeaderMap::new(), None));
    }

    #[test]
    fn test_auth_is_enabled_true_when_keys_set() {
        let auth = AuthState::new(vec!["key".to_string()]);
        assert!(auth.is_enabled());
    }

    #[test]
    fn test_auth_via_query_key() {
        // Provide the API key via the URL query parameter instead of header.
        let auth = AuthState::new(vec!["query-secret".to_string()]);
        assert!(auth.check(&HeaderMap::new(), Some("query-secret")));
    }

    #[test]
    fn test_auth_invalid_query_key() {
        let auth = AuthState::new(vec!["query-secret".to_string()]);
        assert!(!auth.check(&HeaderMap::new(), Some("wrong")));
    }

    #[test]
    fn test_auth_header_takes_precedence_over_query_key() {
        let auth = AuthState::new(vec!["header-secret".to_string()]);
        let mut headers = HeaderMap::new();
        headers.insert("X-API-Key", "header-secret".parse().unwrap());
        // Header has correct key, query has wrong — should still pass.
        assert!(auth.check(&headers, Some("wrong")));
    }
}
