import re
import fetch_brand_from_amazon

# ------------------------------------------
# Predefined list of common colors
# ------------------------------------------

GENDER_TOKENS = [
    "Men", "Men's", "Women", "Women's", "Boys", "Girls", "Kids", "Kid's"
]

BRANDS = [
    "Red Tape", "boAt", "Sony", "Samsung", "JBL", "Noise", "Boult",
    "Apple", "OnePlus", "Puma", "Adidas", "Nike", "Allen Solly",
    "Hush Puppies", "Red Chief", "Woodland", "U.S. Polo", "HRX"
]

COLORS = [
    "Black", "White", "Blue", "Red", "Green", "Yellow", "Brown", "Grey", "Gray",
    "Pink", "Purple", "Orange", "Beige", "Silver", "Gold", "Maroon", "Navy",
    "Teal", "Olive", "Cream", "Multicolor", "Multi"
]

# Acceptable multi-color formats
# Examples: Black-Gold, Navy Blue, Rose Gold
MULTI_COLOR_PATTERNS = [
    r"\b(" + "|".join(COLORS) + r")[- ](" + "|".join(COLORS) + r")\b"
]


# ------------------------------------------
# Extract Brand (Assumption: First word or first two words)
# ------------------------------------------
def extract_brand(name):
    # 1. Try matching known brands exactly
    for brand in BRANDS:
        if name.lower().startswith(brand.lower()):
            return brand

    # 2. If not in list, detect brand until a gender word appears
    words = name.split()

    brand_tokens = []
    for w in words:
        if any(g.lower() in w.lower() for g in GENDER_TOKENS):
            break
        brand_tokens.append(w)

    # Brand = first token usually
    if len(brand_tokens) >= 1:
        return brand_tokens[0]

    # Fallback
    return words[0]


# ------------------------------------------
# Extract Color
# ------------------------------------------
def extract_color(name, brand):
    # 1. Remove brand words from title
    safe_name = name.replace(brand, "", 1)

    # 2. First detect multi-color formats (Black-Gold, Navy Blue, etc.)
    for pattern in MULTI_COLOR_PATTERNS:
        match = re.search(pattern, safe_name, re.IGNORECASE)
        if match:
            return match.group(0).strip()

    # 3. Look for simple colors (after removing brand)
    for color in COLORS:
        pattern = r"\b" + re.escape(color) + r"\b"
        if re.search(pattern, safe_name, re.IGNORECASE):
            return color

    return ""


# ------------------------------------------
# Extract Dimensions (like 54 x 46 x 28 cm)
# ------------------------------------------
def extract_dimensions(name):
    match = re.search(r"(\d+\s*x\s*\d+\s*x\s*\d+\s*(cm|mm|in|inch|inches))", name, re.IGNORECASE)
    return match.group(1) if match else ""


# ------------------------------------------
# Extract Pack Count (e.g., "2 Pack", "Pack of 4")
# ------------------------------------------
def extract_pack_count(name):
    match1 = re.search(r"(\d+)\s*Pack", name, re.IGNORECASE)
    if match1:
        return match1.group(1)

    match2 = re.search(r"Pack of\s*(\d+)", name, re.IGNORECASE)
    if match2:
        return match2.group(1)

    return ""


# ------------------------------------------
# Final function to parse attributes
# ------------------------------------------
def parse_product_attributes(name, url=None):
    brand = extract_brand(name)
    if url is not None:
        brand = fetch_brand_from_amazon.extract_brand_from_amazon(url)
    if brand is None:
        return None
    color = extract_color(name, brand)
    return {
        "brand": brand,
        "color": color,
        "dimensions": extract_dimensions(name),
        "pack_count": extract_pack_count(name)
    }


# ------------------------------------------
# Test With Sample Input
# ------------------------------------------
sample = "Storite 2 Pack Moisture Proof Nylon Large Size Underbed Storage Bag (54 x 46 x 28 cm) - black, Recta..."
sample2 = "Puma Unisex-Child Tacto Ii Fg/Ag Junior Football Shoe"
sample3 = "boAt Airdopes 141 Bluetooth Truly Wireless in Ear Earbuds with 42H Playtime, Beast Mode(Low Latency .."
sample4 = "Vector X Blaze-2.0 Football Shoes for Men's | Black-Gold | Size-9 | for Men and Adult | Walking | Casual | Lace-Up |"

print(parse_product_attributes(sample4))
