use uuid::Uuid;

pub fn generate_key() -> String {
    let uuid = Uuid::new_v4();
    return uuid.to_string();
}
