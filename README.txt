Website Logo Similarity Clustering Challenge
This project groups websites by logo similarity without using complex Machine Learning (ML) algorithms like k-means or DBSCAN.
The solution relies on Perceptual Hashing (dHash) for feature extraction and the Union-Find algorithm for non-ML clustering.
Technical Approach
1. Data Access
* Goal: Read domain list from the Snappy-compressed Parquet file.
* Method: Used DuckDB (analytical database).
* Benefit: Allows direct SQL querying of Parquet, simplifying data access over complex Apache libraries.
2. Logo Extraction (Result: 89%)
* Tool: Jsoup (Java library) for efficient HTML parsing.
* Strategy: Extractor searches common logo tags (og:image, link[rel=icon]) and structure (<header>, "logo" classes).
* Format Support: Added logic to rasterize SVGs and decode .ico files using Apache libraries.
* Note on Rate: Achieved an 89% extraction rate. This demonstrates robust scraping logic. The missing 11% highlights the complexity of modern, CSS-heavy web designs.
Non-ML Clustering Solution
The core challenge was visual similarity grouping.
1. Similarity Measurement: Perceptual Hashing (dHash)
* Method: Generates a 64-bit hash ('DNA') for each logo.
* Process: Logo is resized to 9 x 8 pixels. The hash is based on the gradient/edge direction (luminance difference between adjacent pixels).
* Benefit: Visually similar logos have minimal difference in their hashes, regardless of scale or compression.
2. Clustering: Hamming Distance & Union-Find
* Similarity Score: Hamming Distance counts the bit differences between two hashes.
* Logic: A fixed threshold (8 bits) defines a match.
* Grouping: The Union-Find algorithm efficiently connects all matching logos into clusters. This is a scalable, graph-based clustering method used instead of k-means or DBSCAN.
* Optimization: A bucketing system reduces the number of pair-wise comparisons, improving scalability.
Tech Stack
* Language: Java
* Key Libraries:
o Jsoup (HTML Parsing)
o OkHttp (Web Fetching)
o DuckDB JDBC (Parquet Data Access)
o Apache Batik and Commons Imaging (Image/SVG Decoding)

