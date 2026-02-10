# test_load_models.py
import pickle, traceback

paths = ["models/regressor_pipeline.pkl", "models/classifier_pipeline.pkl"]
for p in paths:
    print("Trying:", p)
    try:
        with open(p, "rb") as f:
            m = pickle.load(f)
        print(" OK — loaded:", type(m))
        if hasattr(m, "named_steps"):
            print(" pipeline steps:", list(m.named_steps.keys()))
        if hasattr(m, "feature_names_in_"):
            print(" feature_names_in_ (sample):", getattr(m, "feature_names_in_")[:10])
    except Exception as e:
        print(" FAILED to load:", e)
        traceback.print_exc()
    print("-" * 40)
