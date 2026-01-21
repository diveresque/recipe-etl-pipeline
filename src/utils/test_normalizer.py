from src.transform.ingredient_normalizer import normalize_ingredient_name, normalize_measure

def test(name, measure=None):
    print(f"\nRAW: {name!r} / {measure!r}")
    print(f"NORMALIZED NAME: {normalize_ingredient_name(name)!r}")
    if measure is not None:
        print(f"NORMALIZED MEASURE: {normalize_measure(measure)!r}")

if __name__ == "__main__":

    print("=== Testing ingredient normalization ===")

    tests = [
        ("Eggs", "2"),
        ("EGGS", "3 large"),
        ("egg", "1"),
        ("tomatoes", "3"),
        ("potatoes", "2"),
        ("   butter   ", "1 tbsp"),
        ("3 oz of pearl tapioca", "3 oz"),
        ("4 large onions", "4 large"),
        ("1 1/2 cups flour", "1 1/2 cups"),
        ("N/A", None),
        ("", "2 tbsp"),
        ("   ", "100 ml"),
        (None, "1 tsp"),
        ("Sea Salt", "to taste"),
        ("icing sugar", "100g"),
        ("pretzels", "75g"),
        ("double cream", "300ml"),
        ("caramel sauce", "drizzle"),
        ("toffee popcorn", "Top"),
    ]

    for name, measure in tests:
        test(name, measure)
