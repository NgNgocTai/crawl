-- MySQL 8+ schema for Price Hawk normalized pricing data.
-- Run with: mysql -u <user> -p <db_name> < sql/mysql_schema.sql

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

CREATE TABLE IF NOT EXISTS platforms (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  base_url VARCHAR(255) NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_platforms_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS categories (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  slug VARCHAR(128) NOT NULL,
  parent_id INT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_categories_slug (slug),
  CONSTRAINT fk_categories_parent FOREIGN KEY (parent_id) REFERENCES categories(id)
    ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS products (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  category_id INT NOT NULL,
  name VARCHAR(512) NOT NULL,
  normalize_name VARCHAR(512) NOT NULL,
  brand VARCHAR(128) NULL,
  model_key VARCHAR(255) NULL,
  variant_key VARCHAR(255) NULL,
  image_url TEXT NULL,
  description TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_products_category_normalize_name (category_id, normalize_name),
  KEY idx_products_model_key (model_key),
  KEY idx_products_brand (brand),
  CONSTRAINT fk_products_category FOREIGN KEY (category_id) REFERENCES categories(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS price_records (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  product_id BIGINT NOT NULL,
  platform_id INT NOT NULL,
  price DECIMAL(15,2) NOT NULL,
  url TEXT NULL,
  in_stock TINYINT(1) NOT NULL DEFAULT 1,
  crawled_at DATETIME(6) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_price_observation (product_id, platform_id, crawled_at),
  KEY idx_price_records_product_platform_crawled (product_id, platform_id, crawled_at),
  KEY idx_price_records_platform_crawled (platform_id, crawled_at),
  CONSTRAINT fk_price_records_product FOREIGN KEY (product_id) REFERENCES products(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_price_records_platform FOREIGN KEY (platform_id) REFERENCES platforms(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET FOREIGN_KEY_CHECKS = 1;

INSERT INTO platforms (name, base_url)
VALUES
  ('fpt', 'https://fptshop.com.vn'),
  ('tgdd', 'https://www.thegioididong.com'),
  ('hoangha', 'https://hoanghamobile.com')
ON DUPLICATE KEY UPDATE
  base_url = VALUES(base_url),
  updated_at = CURRENT_TIMESTAMP;
