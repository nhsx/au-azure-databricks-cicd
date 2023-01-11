CREATE TABLE [dbo].[pre_load_log](
	[row_count] [bigint] NULL,
	[load_date] [datetime2](7) NULL,
	[file_to_load] [nvarchar](max) NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
