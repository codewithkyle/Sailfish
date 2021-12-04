CREATE TABLE framework(
    id INTEGER PRIMARY KEY DEFAULT 0 CHECK(id >= 0)
);
CREATE TABLE consumers(
    uid NVARCHAR(36) PRIMARY KEY,
    file_number INTEGER NOT NULL DEFAULT 0 CHECK(file_number >= 0)
);
