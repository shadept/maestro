ALTER TABLE "projects" ADD COLUMN "agent" jsonb DEFAULT '{}'::jsonb NOT NULL;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "agent_model" text;--> statement-breakpoint
ALTER TABLE "sessions" ADD COLUMN "agent_effort" text;--> statement-breakpoint
-- FUR-41 backfill: TaskContext gained required (nullable) agentModel/agentEffort
-- keys; stored contexts from earlier turns must keep decoding strictly.
UPDATE "task_runs" SET "context" = "context" || '{"agentModel":null,"agentEffort":null}'::jsonb WHERE NOT ("context" ? 'agentModel');
