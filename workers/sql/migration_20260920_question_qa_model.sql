-- Apply before deploying the Worker that persists question Q&A model names.
-- Existing Q&A was generated with Gemini 3 Flash (confirmed by the site owner).
BEGIN;

ALTER TABLE public.question_qa ADD COLUMN IF NOT EXISTS model text;
UPDATE public.question_qa
SET model = 'gemini-3-flash-preview'
WHERE model IS NULL OR btrim(model) = '';

-- An older Worker can still write during rollout; its model was Gemini 3 Flash.
ALTER TABLE public.question_qa ALTER COLUMN model SET DEFAULT 'gemini-3-flash-preview';
COMMENT ON COLUMN public.question_qa.model IS 'Gemini API model ID actually used for this Q&A; legacy rows used Gemini 3 Flash.';

COMMIT;
