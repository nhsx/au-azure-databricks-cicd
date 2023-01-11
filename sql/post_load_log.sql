CREATE TABLE [dbo].[post_load_log](
	[load_date] [datetime2](7) NULL,
	[tbl_name] [nvarchar](800) NULL,
	[aggregation] [nvarchar](50) NULL,
	[aggregate_value] [bigint] NULL
) ON [PRIMARY]
GO