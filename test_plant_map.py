#!/usr/bin/env python3
"""
test_plant_map.py — Quick test script to verify Plant Map implementation
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

def test_plant_layout():
    """Test that plant_layout.json is valid"""
    print("\n" + "="*60)
    print("✓ Testing plant_layout.json...")
    print("="*60)

    layout_path = ROOT / "db" / "plant_layout.json"
    if not layout_path.exists():
        print("❌ plant_layout.json not found!")
        return False

    try:
        with open(layout_path, 'r') as f:
            layout = json.load(f)
    except Exception as e:
        print(f"❌ Failed to parse JSON: {e}")
        return False

    # Validate structure
    required_keys = ['metadata', 'sections', 'inverter_locations', 'inverter_details']
    for key in required_keys:
        if key not in layout:
            print(f"❌ Missing key: {key}")
            return False

    # Count inverters
    num_inverters = len(layout['inverter_locations'])
    print(f"✓ Found {num_inverters} inverters")

    if num_inverters != 36:
        print(f"⚠️  Warning: Expected 36 inverters, found {num_inverters}")

    # Verify all sections
    sections = layout['sections']
    print(f"✓ Found {len(sections)} sections")
    for section in sections:
        inv_count = len(section['inverters'])
        print(f"  - {section['name']}: {inv_count} inverters")

    # Verify coordinates exist
    for inv_id, loc in list(layout['inverter_locations'].items())[:3]:
        if 'x' in loc and 'y' in loc:
            print(f"✓ {inv_id}: ({loc['x']}, {loc['y']})")
        else:
            print(f"❌ {inv_id}: Missing coordinates")
            return False

    print("\n✅ plant_layout.json is valid!")
    return True


def test_plant_map_helpers():
    """Test that plant_map_helpers.py can be imported and functions work"""
    print("\n" + "="*60)
    print("✓ Testing plant_map_helpers.py...")
    print("="*60)

    try:
        from db.plant_map_helpers import (
            load_plant_layout,
            calculate_string_health,
            get_inverter_health_overview,
            get_inverter_strings_detail,
            get_plant_overview
        )
        print("✓ All functions imported successfully")
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False

    # Test load_plant_layout
    try:
        layout = load_plant_layout()
        if layout:
            print(f"✓ load_plant_layout() returned {len(layout.get('inverter_locations', {}))} inverters")
        else:
            print("❌ load_plant_layout() returned empty dict")
            return False
    except Exception as e:
        print(f"❌ load_plant_layout() failed: {e}")
        return False

    # Test inverter health overview
    try:
        health = get_inverter_health_overview("TX1-01")
        if 'error' not in health:
            print(f"✓ get_inverter_health_overview() for TX1-01:")
            print(f"  - Health Score: {health.get('health_score', 'N/A')}")
            print(f"  - Status: {health.get('health_status', 'N/A')}")
        else:
            print(f"⚠️  Warning: {health['error']}")
    except Exception as e:
        print(f"⚠️  Warning: get_inverter_health_overview() failed: {e}")
        print("  (This is expected if database has no metrics yet)")

    # Test strings detail
    try:
        strings = get_inverter_strings_detail("TX1-01")
        if 'error' not in strings:
            print(f"✓ get_inverter_strings_detail() for TX1-01:")
            print(f"  - Total Strings: {strings.get('num_strings', 0)}")
            if strings.get('strings'):
                first_string = strings['strings'][0]
                print(f"  - Sample String: {first_string.get('string_id')}")
        else:
            print(f"⚠️  Warning: {strings['error']}")
    except Exception as e:
        print(f"❌ get_inverter_strings_detail() failed: {e}")
        return False

    print("\n✅ plant_map_helpers.py is working!")
    return True


def test_api_routes():
    """Test that plant_map_routes.py exists and can be imported"""
    print("\n" + "="*60)
    print("✓ Testing plant_map_routes.py...")
    print("="*60)

    try:
        from dashboard.plant_map_routes import router
        print("✓ plant_map_routes.py imported successfully")
        print(f"✓ Router has {len(router.routes)} routes defined")
        for route in router.routes:
            print(f"  - {route.path}")
        return True
    except Exception as e:
        print(f"❌ Failed to import: {e}")
        return False


def test_static_files():
    """Test that plant_map.js exists"""
    print("\n" + "="*60)
    print("✓ Testing static files...")
    print("="*60)

    files_to_check = [
        "dashboard/static/plant_map.js",
        "dashboard/static/index.html"
    ]

    all_exist = True
    for file_path in files_to_check:
        full_path = ROOT / file_path
        if full_path.exists():
            size_kb = full_path.stat().st_size / 1024
            print(f"✓ {file_path} ({size_kb:.1f} KB)")
        else:
            print(f"❌ {file_path} not found")
            all_exist = False

    if all_exist:
        print("\n✅ All static files present!")
    return all_exist


def test_dashboard_app():
    """Test that dashboard app includes plant_map_router"""
    print("\n" + "="*60)
    print("✓ Testing dashboard/app.py integration...")
    print("="*60)

    try:
        app_path = ROOT / "dashboard" / "app.py"
        with open(app_path, 'r') as f:
            content = f.read()

        checks = [
            ("plant_map_router import", "from dashboard.plant_map_routes import router"),
            ("router inclusion", "app.include_router(plant_map_router)")
        ]

        all_good = True
        for check_name, check_str in checks:
            if check_str in content:
                print(f"✓ {check_name} found")
            else:
                print(f"❌ {check_name} NOT found")
                all_good = False

        if all_good:
            print("\n✅ dashboard/app.py is properly configured!")
        return all_good

    except Exception as e:
        print(f"❌ Failed to check app.py: {e}")
        return False


def main():
    """Run all tests"""
    print("\n")
    print("╔" + "="*58 + "╗")
    print("║" + " "*58 + "║")
    print("║" + "  Plant Map Implementation Test Suite".center(58) + "║")
    print("║" + " "*58 + "║")
    print("╚" + "="*58 + "╝")

    tests = [
        ("plant_layout.json", test_plant_layout),
        ("plant_map_helpers.py", test_plant_map_helpers),
        ("plant_map_routes.py", test_api_routes),
        ("Static Files", test_static_files),
        ("Dashboard Integration", test_dashboard_app),
    ]

    results = {}
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"\n❌ Test '{test_name}' crashed: {e}")
            results[test_name] = False

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:8} | {test_name}")

    print("="*60)
    print(f"\nResult: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Plant Map is ready to use.")
        print("\nNext steps:")
        print("1. Run: python dashboard/app.py")
        print("2. Open dashboard in browser")
        print("3. Click 'Plant Map' tab")
        print("4. You should see the interactive map with 36 inverters")
        return 0
    else:
        print("\n⚠️  Some tests failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
