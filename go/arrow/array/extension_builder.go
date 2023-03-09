package array

type ExtensionBuilderWrapper interface {
	NewBuilder(bldr *ExtensionBuilder) Builder
}
