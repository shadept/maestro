ALTER TABLE "outbox" ADD COLUMN "next_attempt_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "projects" ADD COLUMN "linear_team_key" text;--> statement-breakpoint
ALTER TABLE "projects" ADD CONSTRAINT "projects_linear_team_key_unique" UNIQUE("linear_team_key");