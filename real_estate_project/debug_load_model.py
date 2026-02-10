# debug_load_model.py
import os, traceback
MODEL_PATH = "models/regressor_pipeline.pkl"

print("Model path:", MODEL_PATH)
print("Exists:", os.path.exists(MODEL_PATH))
if os.path.exists(MODEL_PATH):
    print("Size (bytes):", os.path.getsize(MODEL_PATH))

try:
    import pickle
    with open(MODEL_PATH, "rb") as f:
        m = pickle.load(f)
    print("✅ pickle.load succeeded. Type:", type(m))
    try:
        print("Has named_steps:", hasattr(m, "named_steps"))
        if hasattr(m, "feature_names_in_"):
            print("feature_names_in_ length:", len(getattr(m, "feature_names_in_")))
    except Exception:
        pass
except Exception:
    print("❌ Exception while pickle.load():")
    traceback.print_exc()
