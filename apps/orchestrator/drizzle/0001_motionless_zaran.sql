ALTER TABLE "task_runs" ADD COLUMN "context" jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "task_runs" ADD COLUMN "result_text" text;