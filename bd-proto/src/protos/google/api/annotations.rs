// This file is generated by rust-protobuf 4.0.0-alpha.0. Do not edit
// .proto file is parsed by protoc 29.3
// @generated

// https://github.com/rust-lang/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

#![allow(unused_attributes)]
#![cfg_attr(rustfmt, rustfmt::skip)]

#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unused_results)]
#![allow(unused_mut)]

//! Generated file from `google/api/annotations.proto`

/// Generated files are compatible only with the same version
/// of protobuf runtime.
const _PROTOBUF_VERSION_CHECK: () = ::protobuf::VERSION_4_0_0_ALPHA_0;

/// Extension fields
pub mod exts {

    pub const http: ::protobuf::ext::ExtFieldOptional<::protobuf::descriptor::MethodOptions, super::super::http::HttpRule> = ::protobuf::ext::ExtFieldOptional::new(72295728, ::protobuf::descriptor::field_descriptor_proto::Type::TYPE_MESSAGE);
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x1cgoogle/api/annotations.proto\x12\ngoogle.api\x1a\x15google/api/htt\
    p.proto\x1a\x20google/protobuf/descriptor.proto:K\n\x04http\x18\xb0\xca\
    \xbc\"\x20\x01(\x0b2\x14.google.api.HttpRule\x12\x1e.google.protobuf.Met\
    hodOptionsR\x04httpBn\n\x0ecom.google.apiB\x10AnnotationsProtoP\x01ZAgoo\
    gle.golang.org/genproto/googleapis/api/annotations;annotations\xa2\x02\
    \x04GAPIb\x06proto3\
";

/// `FileDescriptorProto` object which was a source for this generated file
fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    static file_descriptor_proto_lazy: ::protobuf::rt::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::rt::Lazy::new();
    file_descriptor_proto_lazy.get(|| {
        ::protobuf::Message::parse_from_bytes(file_descriptor_proto_data).unwrap()
    })
}

/// `FileDescriptor` object which allows dynamic access to files
pub fn file_descriptor() -> &'static ::protobuf::reflect::FileDescriptor {
    static generated_file_descriptor_lazy: ::protobuf::rt::Lazy<::protobuf::reflect::GeneratedFileDescriptor> = ::protobuf::rt::Lazy::new();
    static file_descriptor: ::protobuf::rt::Lazy<::protobuf::reflect::FileDescriptor> = ::protobuf::rt::Lazy::new();
    file_descriptor.get(|| {
        let generated_file_descriptor = generated_file_descriptor_lazy.get(|| {
            let mut deps = ::std::vec::Vec::with_capacity(2);
            deps.push(super::http::file_descriptor().clone());
            deps.push(::protobuf::descriptor::file_descriptor().clone());
            let mut messages = ::std::vec::Vec::with_capacity(0);
            let mut enums = ::std::vec::Vec::with_capacity(0);
            ::protobuf::reflect::GeneratedFileDescriptor::new_generated(
                file_descriptor_proto(),
                deps,
                messages,
                enums,
            )
        });
        ::protobuf::reflect::FileDescriptor::new_generated_2(generated_file_descriptor)
    })
}
