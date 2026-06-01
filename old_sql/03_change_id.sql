ALTER TABLE public.mock_data RENAME COLUMN id TO source_id;

ALTER TABLE public.mock_data ADD COLUMN id SERIAL;

ALTER TABLE public.mock_data ADD PRIMARY KEY (id);

ALTER TABLE mock_data DROP COLUMN source_id;