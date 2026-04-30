using UnrealBuildTool;
using System.IO;

public class Quilkin : ModuleRules
{
	public Quilkin(ReadOnlyTargetRules Target) : base(Target)
	{
		PCHUsage = PCHUsageMode.UseExplicitOrSharedPCHs;
		bStaticAnalyzerExtensions = true;

		PrivateDependencyModuleNames.AddRange(new string[] {
			"Cbor",
			"Json",
			"HTTP",
			"Networking",
		});

		PublicDependencyModuleNames.AddRange(new string[] {
			"Sockets",
			"Core", 
			"CoreUObject", 
			"Engine", 
			"DeveloperSettings",
			"InputCore",
		});

		PrivateIncludePaths.AddRange(new string[] {
			"Quilkin/Private/Tests",
		});
	}
}
