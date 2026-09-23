BEGIN;

ALTER TABLE public.deep_dive_explanations ADD COLUMN IF NOT EXISTS model_name text;
ALTER TABLE public.deep_dive_explanations ADD COLUMN IF NOT EXISTS review_status text NOT NULL DEFAULT 'ai';

-- The site owner identified existing deep dives as Gemini 3 Flash.
-- Generation alone does not imply an AI fact check or teacher approval.
UPDATE public.deep_dive_explanations SET model_name = 'Gemini 3 Flash'
WHERE coalesce(btrim(model_name), '') = '';
UPDATE public.deep_dive_explanations SET review_status = 'ai'
WHERE coalesce(btrim(review_status), '') = '';

COMMENT ON COLUMN public.deep_dive_explanations.model_name IS 'Generation model; legacy deep dives used Gemini 3 Flash.';
COMMENT ON COLUMN public.deep_dive_explanations.review_status IS 'Same as normal explanations: ai, ai_fact_checked, teacher_approved, teacher_edited.';

NOTIFY pgrst, 'reload schema';
COMMIT;
