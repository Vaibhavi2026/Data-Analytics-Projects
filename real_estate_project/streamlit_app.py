# streamlit_app.py
import streamlit as st
import pandas as pd
import numpy as np
import os
import traceback

# Try multiple loaders for robustness
import pickle
try:
    import joblib
except Exception:
    joblib = None
try:
    import cloudpickle
except Exception:
    cloudpickle = None

st.set_page_config(page_title="Real Estate Investment Advisor", layout="wide")

# -------------------------
# Robust loader
# -------------------------
def try_load_model(path):
    """
    Try to load model using pickle, then joblib, then cloudpickle.
    Returns (model_or_None, error_traceback_or_None)
    """
    if not os.path.exists(path):
        return None, f"File not found: {path}"
    # 1) pickle
    try:
        with open(path, "rb") as f:
            model = pickle.load(f)
        return model, None
    except Exception:
        err_pickle = traceback.format_exc()

    # 2) joblib
    if joblib is not None:
        try:
            model = joblib.load(path)
            return model, None
        except Exception:
            err_joblib = traceback.format_exc()
    else:
        err_joblib = "joblib not installed"

    # 3) cloudpickle
    if cloudpickle is not None:
        try:
            with open(path, "rb") as f:
                model = cloudpickle.load(f)
            return model, None
        except Exception:
            err_cloud = traceback.format_exc()
    else:
        err_cloud = "cloudpickle not installed"

    combined = f"pickle error:\n{err_pickle}\n\njoblib error:\n{err_joblib}\n\ncloudpickle error:\n{err_cloud}"
    return None, combined

# -------------------------
# Load processed CSV (cached)
# -------------------------
@st.cache_data
def load_processed_data(path="data/final_data.csv"):
    try:
        return pd.read_csv(path)
    except Exception:
        return None

# -------------------------
# Constants and model loading
# -------------------------
MODEL_REG_PATH = "models/regressor_pipeline.pkl"
MODEL_CLF_PATH = "models/classifier_pipeline.pkl"
PROCESSED_CSV = "data/final_data.csv"

reg_pipeline, reg_err = try_load_model(MODEL_REG_PATH)
clf_pipeline, clf_err = try_load_model(MODEL_CLF_PATH)
processed_df = load_processed_data(PROCESSED_CSV)

# -------------------------
# Infer feature list
# -------------------------
def infer_feature_list_from_models(processed_df, reg_model, clf_model):
    """
    Priority:
      1) model.feature_names_in_ on reg_model or clf_model
      2) processed_df columns excluding known target columns
    """
    for m in (reg_model, clf_model):
        if m is not None and hasattr(m, "feature_names_in_"):
            try:
                return list(getattr(m, "feature_names_in_"))
            except Exception:
                pass
    if processed_df is not None:
        cols = list(processed_df.columns)
        for t in ["Good_Investment", "Future_Price_5Yrs"]:
            if t in cols:
                cols.remove(t)
        return cols
    return None

feature_list = infer_feature_list_from_models(processed_df, reg_pipeline, clf_pipeline)

# -------------------------
# Utilities
# -------------------------
def align_inputs_to_features(input_df: pd.DataFrame, feature_list: list, processed_df=None):
    """
    Align input_df to required feature_list:
    - Rename common user-friendly names to expected names
    - Add missing columns with sensible defaults
    - Reorder columns to match feature_list
    """
    df = input_df.copy()

    # rename heuristics (case-insensitive)
    rename_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in ("sqft", "size", "size_in_sqft"):
            rename_map[c] = "Size_in_SqFt"
        if lc == "bhk":
            rename_map[c] = "BHK"
        if lc in ("year", "year_built"):
            rename_map[c] = "Year_Built"
        if lc in ("floor", "floor_no"):
            rename_map[c] = "Floor_No"
        if lc in ("total_floors", "totalfloor"):
            rename_map[c] = "Total_Floors"
        if lc in ("age", "age_of_property"):
            rename_map[c] = "Age_of_Property"
        if lc == "id":
            rename_map[c] = "ID"
    if rename_map:
        df = df.rename(columns=rename_map)

    # Defaults: numeric -> median or 0, categorical -> empty string
    defaults = {}
    numeric_cols = []
    if processed_df is not None:
        numeric_cols = processed_df.select_dtypes(include=[np.number]).columns.tolist()

    for col in feature_list:
        if col not in df.columns:
            if col in numeric_cols:
                try:
                    defaults[col] = float(processed_df[col].median())
                except Exception:
                    defaults[col] = 0.0
            else:
                defaults[col] = ""
            df[col] = defaults[col]

    # Reorder to match requested feature_list
    df = df[[c for c in feature_list]]

    # Coerce numeric columns
    for c in df.columns:
        try:
            if processed_df is not None and c in numeric_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(defaults.get(c, 0))
            else:
                df[c] = pd.to_numeric(df[c], errors="ignore")
        except Exception:
            pass

    return df

def predict_with_model(model, X_df):
    """
    Accept either a sklearn Pipeline (with preprocessor) or a raw estimator.
    Returns numpy array of predictions.
    """
    if model is None:
        raise ValueError("Model is None")

    # Pipeline case
    if hasattr(model, "named_steps"):
        return model.predict(X_df)

    # Raw estimator with feature_names_in_
    if hasattr(model, "feature_names_in_"):
        expected = list(getattr(model, "feature_names_in_"))
        X_aligned = X_df.copy()
        for c in expected:
            if c not in X_aligned.columns:
                X_aligned[c] = 0
        X_aligned = X_aligned[expected]
        return model.predict(X_aligned)

    # Fallback
    return model.predict(X_df)

def predict_proba_if_available(model, X_df):
    """Return probability of positive class if available, else None"""
    if model is None:
        return None
    try:
        # pipeline may expose predict_proba
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(X_df)
            return [float(p[1]) if len(p) > 1 else 0.0 for p in proba]
        if hasattr(model, "named_steps"):
            final = list(model.named_steps.values())[-1]
            if hasattr(final, "predict_proba"):
                try:
                    X_trans = model.transform(X_df)
                except Exception:
                    try:
                        X_trans = list(model.named_steps.values())[0].transform(X_df)
                    except Exception:
                        X_trans = X_df
                proba = final.predict_proba(X_trans)
                return [float(p[1]) if len(p) > 1 else 0.0 for p in proba]
    except Exception:
        return None
    return None

def regressor_uncertainty_if_rf(model, X_df):
    """If final regressor is RandomForest, estimate std across tree predictions."""
    try:
        if model is None:
            return None
        if hasattr(model, "named_steps"):
            final = list(model.named_steps.values())[-1]
            try:
                X_trans = model.transform(X_df)
            except Exception:
                try:
                    X_trans = list(model.named_steps.values())[0].transform(X_df)
                except Exception:
                    X_trans = X_df
            if hasattr(final, "estimators_"):
                preds_trees = np.vstack([est.predict(X_trans) for est in final.estimators_])
                stds = preds_trees.std(axis=0)
                return stds
        else:
            if hasattr(model, "estimators_"):
                expected = getattr(model, "feature_names_in_", None)
                X_aligned = X_df.copy()
                if expected is not None:
                    for c in expected:
                        if c not in X_aligned.columns:
                            X_aligned[c] = 0
                    X_aligned = X_aligned[expected]
                preds_trees = np.vstack([est.predict(X_aligned) for est in model.estimators_])
                stds = preds_trees.std(axis=0)
                return stds
    except Exception:
        return None
    return None

# -------------------------
# Sidebar: status & reload
# -------------------------
with st.sidebar:
    st.header("Configuration & Status")
    st.write("Model paths:")
    st.write(f"- Regression: `{MODEL_REG_PATH}`")
    st.write(f"- Classification: `{MODEL_CLF_PATH}`")
    st.write(f"- Processed CSV: `{PROCESSED_CSV}`")
    st.markdown("---")
    st.write("Loaded status:")
    st.write(f"Regression model: {'✅' if reg_pipeline is not None else '❌'}")
    st.write(f"Classification model: {'✅' if clf_pipeline is not None else '❌'}")
    st.write(f"Processed CSV: {'✅' if processed_df is not None else '❌'}")
    if reg_err:
        st.text("Regression load error (short):")
        try:
            st.code(str(reg_err).splitlines()[-1])
        except Exception:
            st.code(str(reg_err))
    if clf_err:
        st.text("Classification load error (short):")
        try:
            st.code(str(clf_err).splitlines()[-1])
        except Exception:
            st.code(str(clf_err))
    st.markdown("---")
    if st.button("Reload / restart app", key="reload_btn"):
        st.experimental_rerun()

# -------------------------
# Main UI
# -------------------------
st.title("Real Estate Investment Advisor — Classification & 5-year Price Forecast")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Single property input")

    State = st.text_input("State", value="", key="s_state")
    City = st.text_input("City", value="", key="s_city")
    Locality = st.text_input("Locality", value="", key="s_locality")
    Property_Type = st.selectbox("Property Type", options=["Apartment","Villa","House","Plot",""], index=0, key="s_ptype")
    BHK = st.selectbox("BHK", options=[1,2,3,4,5], index=1, key="s_bhk")
    Size_in_SqFt = st.number_input("Size (SqFt)", min_value=50.0, value=750.0, step=10.0, key="s_size")
    Year_Built = st.number_input("Year Built", min_value=1900, max_value=2050, value=2020, key="s_year")
    Furnished_Status = st.selectbox("Furnished Status", options=["Unfurnished","Semi","Fully",""], index=0, key="s_furnish")
    Floor_No = st.number_input("Floor No", min_value=0, max_value=200, value=1, key="s_floor")
    Total_Floors = st.number_input("Total Floors", min_value=1, max_value=200, value=3, key="s_total")
    Nearby_Schools = st.number_input("Nearby Schools (count/score)", min_value=0, value=1, key="s_schools")
    Nearby_Hospitals = st.number_input("Nearby Hospitals (count)", min_value=0, value=1, key="s_hospitals")
    Public_Transport_Accessibility = st.selectbox("Public Transport Access", options=["Poor","Average","Good","Excellent",""], index=1, key="s_transport")
    Parking_Space = st.number_input("Parking spaces", min_value=0, value=1, key="s_parking")
    Security = st.selectbox("Security", options=["None","Gated","CCTV","Guard","Other",""], index=0, key="s_security")
    Amenities = st.text_input("Amenities (comma-separated)", value="", key="s_amenities")
    Facing = st.selectbox("Facing", options=["North","South","East","West",""], index=0, key="s_facing")
    Owner_Type = st.selectbox("Owner Type", options=["Individual","Builder","Agent",""], index=0, key="s_owner")
    Availability_Status = st.selectbox("Availability Status", options=["Available","Under Construction","Sold",""], index=0, key="s_avail")
    ID = st.text_input("ID (optional)", value="", key="s_id")

# Single predict
if st.button("Predict (single)", key="predict_single"):
    if feature_list is None:
        st.error("Cannot infer feature list. Ensure data/final_data.csv exists or models contain feature_names_in_.")
    else:
        input_dict = {
            "State": State,
            "City": City,
            "Locality": Locality,
            "Property_Type": Property_Type,
            "BHK": BHK,
            "Size_in_SqFt": Size_in_SqFt,
            "Year_Built": Year_Built,
            "Furnished_Status": Furnished_Status,
            "Floor_No": Floor_No,
            "Total_Floors": Total_Floors,
            "Nearby_Schools": Nearby_Schools,
            "Nearby_Hospitals": Nearby_Hospitals,
            "Public_Transport_Accessibility": Public_Transport_Accessibility,
            "Parking_Space": Parking_Space,
            "Security": Security,
            "Amenities": Amenities,
            "Facing": Facing,
            "Owner_Type": Owner_Type,
            "Availability_Status": Availability_Status,
            "ID": ID if ID != "" else 0
        }
        df_single_raw = pd.DataFrame([input_dict])
        X_single = align_inputs_to_features(df_single_raw, feature_list, processed_df)

        # Classification
        if clf_pipeline is None:
            st.warning("Classification model not available. Train and save models/classifier_pipeline.pkl")
        else:
            try:
                clf_pred = predict_with_model(clf_pipeline, X_single)
                proba = predict_proba_if_available(clf_pipeline, X_single)
                st.markdown("#### Classification — Good Investment")
                is_good = int(clf_pred[0])
                st.write("Prediction:", "✅ Good Investment" if is_good == 1 else "❌ Not a Good Investment")
                if proba is not None:
                    st.write(f"Confidence (probability of Good Investment): {proba[0]:.2f}")
            except Exception as e:
                st.error(f"Classification error: {e}")
                st.exception(e)

        # Regression
        if reg_pipeline is None:
            st.warning("Regression model not available. Train and save models/regressor_pipeline.pkl")
        else:
            try:
                reg_pred = predict_with_model(reg_pipeline, X_single)
                stds = regressor_uncertainty_if_rf(reg_pipeline, X_single)
                st.markdown("#### Regression — Estimated Price after 5 years")
                st.write(f"Predicted future price (same units used in training): {float(reg_pred[0]):.2f}")
                if stds is not None:
                    st.write(f"Model uncertainty (std across trees): ±{float(stds[0]):.2f}")
            except Exception as e:
                st.error(f"Regression error: {e}")
                st.exception(e)

with col2:
    st.subheader("Batch predictions (CSV)")
    uploaded_file = st.file_uploader("Upload CSV with raw properties (optional)", type=["csv"], key="batch_upload")

    if uploaded_file is not None:
        try:
            df_batch = pd.read_csv(uploaded_file)
            st.success("Uploaded CSV loaded")
        except Exception as e:
            st.error(f"Failed to read uploaded CSV: {e}")
            df_batch = None
    else:
        if processed_df is not None:
            st.info("No upload detected. Showing first 200 rows from processed data as sample.")
            df_batch = processed_df.head(200)
        else:
            df_batch = None
            st.info("Upload a CSV file or run src/feature_engineering.py to create data/final_data.csv")

    if df_batch is not None:
        st.write("Preview:")
        st.dataframe(df_batch.head())

        if st.button("Predict all (batch)", key="predict_batch_btn"):
            if feature_list is None:
                st.error("Feature list unknown; cannot run batch predictions.")
            else:
                try:
                    X_batch = align_inputs_to_features(df_batch, feature_list, processed_df)
                    df_out = df_batch.copy()

                    # Classification
                    if clf_pipeline is not None:
                        clf_preds = predict_with_model(clf_pipeline, X_batch)
                        clf_prob = predict_proba_if_available(clf_pipeline, X_batch)
                        df_out["Good_Investment_Pred"] = clf_preds
                        if clf_prob is not None:
                            df_out["Good_Investment_Prob"] = clf_prob

                    # Regression
                    if reg_pipeline is not None:
                        reg_preds = predict_with_model(reg_pipeline, X_batch)
                        df_out["Future_Price_5Yrs_Pred"] = reg_preds

                    st.success("Batch predictions complete — preview below")
                    st.dataframe(df_out.head())

                    csv = df_out.to_csv(index=False).encode("utf-8")
                    st.download_button("Download predictions CSV", data=csv, file_name="predictions.csv", mime="text/csv", key="download_preds_btn")
                except Exception as e:
                    st.error(f"Batch prediction failed: {e}")
                    st.exception(e)

    st.markdown("---")
    st.subheader("Model insights & feature importance (best-effort)")
    if reg_pipeline is not None:
        try:
            # Attempt to extract feature names from preprocessor if pipeline, else fallback to feature_list
            if hasattr(reg_pipeline, "named_steps"):
                preproc = list(reg_pipeline.named_steps.values())[0]
                try:
                    feature_names = preproc.get_feature_names_out()
                except Exception:
                    feature_names = feature_list
                final_est = list(reg_pipeline.named_steps.values())[-1]
                if feature_names is not None and len(feature_names) == len(importances := getattr(final_est, "feature_importances_", [])):
                    imp_df = pd.DataFrame({"feature": feature_names, "importance": importances})
                    imp_df = imp_df.sort_values("importance", ascending=False).head(15)
                    st.write("Top feature importances (regressor):")
                    st.table(imp_df.reset_index(drop=True))
                else:
                    st.write("Feature importance length mismatch; showing numeric columns from processed data.")
                    if processed_df is not None:
                        st.write(processed_df.select_dtypes(include=[np.number]).columns.tolist()[:15])
                    else:
                        st.write("No processed_df available.")
            else:
                final_est = reg_pipeline
                if hasattr(final_est, "feature_importances_"):
                    importances = final_est.feature_importances_
                    if feature_list is not None and len(feature_list) == len(importances):
                        imp_df = pd.DataFrame({"feature": feature_list, "importance": importances}).sort_values("importance", ascending=False).head(15)
                        st.write("Top feature importances (regressor):")
                        st.table(imp_df.reset_index(drop=True))
                    else:
                        st.write("Cannot align importances to feature names automatically.")
                else:
                    st.write("Regressor does not expose feature_importances_.")
        except Exception as e:
            st.write("Could not compute importances:", e)
    else:
        st.write("No trained regressor loaded to show importances.")

st.markdown("---")
st.write("Developer notes:")
st.write("• This app accepts either sklearn Pipelines (preferred) or raw estimators saved with pickle/joblib/cloudpickle.")
st.write("• If predictions seem wrong, retrain pipelines with proper preprocessing and save them as pipeline pickles/joblib.")
st.write("• Large model files (GBs) may be slow to load; consider saving smaller inference models or using fewer trees.")
