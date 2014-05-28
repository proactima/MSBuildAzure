using System;
using System.IO;
using System.Linq;
using Microsoft.Build.Framework;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace RhysG.MSBuild.Azure
{
	public class CopyToAzureBlobStorageTask : ITask
	{
		[Required]
		public string ContainerName { get; set; }

		[Required]
		public string ConnectionString { get; set; }

		[Required]
		public string ContentType { get; set; }

		[Required]
		public ITaskItem[] Files { get; set; }

		public string ContainerPermission { get; set; }

		public string ContentEncoding { get; set; }

		public IBuildEngine BuildEngine { get; set; }

		public ITaskHost HostObject { get; set; }

		public bool Execute()
		{
			var container = GetBloblContainer();
			if (container == null) return false;

			foreach (var file in Files.Select(fileItem => new FileInfo(fileItem.ItemSpec)))
			{
				BuildEngine.LogMessageEvent(new BuildMessageEventArgs(String.Format("Updating: {0}", file.Name), String.Empty,
					"CopyToAzureBlobStorageTask", MessageImportance.Normal));

				var blob = container.GetBlockBlobReference(file.Name);

				try
				{
					blob.FetchAttributes();
				}
				catch (StorageException)
				{
				}

				if (!IsRemoteFileOlder(file, blob)) continue;

				blob.UploadFromFile(file.FullName, FileMode.Create);

				blob.Properties.ContentType = ContentType;

				if (!String.IsNullOrWhiteSpace(ContentEncoding))
					blob.Properties.ContentEncoding = ContentEncoding;

				blob.Metadata["LastModified"] = file.LastWriteTimeUtc.Ticks.ToString();
				blob.SetMetadata();
				blob.SetProperties();

				BuildEngine.LogMessageEvent(new BuildMessageEventArgs(String.Format("Updating: {0} - Uploaded!", file.Name),
					String.Empty,
					"CopyToAzureBlobStorageTask", MessageImportance.Normal));
			}

			return true;
		}

		private bool IsRemoteFileOlder(FileSystemInfo file, ICloudBlob blob)
		{
			var lastModified = DateTime.MinValue;

			if (!String.IsNullOrWhiteSpace(blob.Metadata["LastModified"]))
			{
				var timeTicks = long.Parse(blob.Metadata["LastModified"]);
				lastModified = new DateTime(timeTicks, DateTimeKind.Utc);
			}

			if (lastModified < file.LastWriteTimeUtc)
			{
				BuildEngine.LogMessageEvent(
					new BuildMessageEventArgs(String.Format("Updating: {0} - Local file is older than remote, skipping", file.Name),
						String.Empty,
						"CopyToAzureBlobStorageTask", MessageImportance.Normal));
				return false;
			}

			return true;
		}

		private CloudBlobContainer GetBloblContainer()
		{
			CloudStorageAccount account;

			if (!CloudStorageAccount.TryParse(ConnectionString, out account)) return null;

			var client = account.CreateCloudBlobClient();

			var container = client.GetContainerReference(ContainerName);
			container.CreateIfNotExists();
			container.SetPermissions(GetPermissions());

			return container;
		}

		private BlobContainerPermissions GetPermissions()
		{
			BlobContainerPublicAccessType accessType;
			return Enum.TryParse(ContainerPermission, out accessType)
				? new BlobContainerPermissions {PublicAccess = accessType}
				: new BlobContainerPermissions {PublicAccess = BlobContainerPublicAccessType.Off};
		}
	}
}