use core::panic;
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufReader,
};

use proc_macro::TokenStream;
use quote::quote;
use serde::Deserialize;
use syn::{LitStr, parse_macro_input};

#[derive(Debug, Clone, Deserialize)]
struct RenoirProgram {
    jobs: Vec<RenoirJob>,
    // configuration: String,
    async_mode: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct RenoirJob {
    // name: String,
    nodes: HashMap<String, RenoirNode>,
    edges: HashMap<String, String>, // TODO: this is a simplification; a node could have multiple successors
}

#[derive(Debug, Clone, Deserialize)]
enum RenoirNode {
    CsvSource { path: String, fields: Vec<String>, types: Vec<String> },
    Filter { condition: String },
    Map { function: String },
    // Join { other_job: String, key: String },
    // Split { count: usize },
    CollectVec {},
}

/// Source nodes are those that do not have any incoming edges.
fn find_source_nodes(job: &RenoirJob) -> Vec<String> {
    let all_nodes: HashSet<String> = job.nodes.iter().map(|(name, _)| name.clone()).collect();
    let successors: HashSet<String> = job.edges.values().cloned().collect(); //flat_map(|s| s.iter()).cloned().collect();
    all_nodes.difference(&successors).cloned().collect()
}

#[proc_macro]
pub fn make_environment(input: TokenStream) -> TokenStream {
    let filepath: String = parse_macro_input!(input as LitStr).value();

    let filereader = BufReader::new(File::open(&filepath).expect("Failed to open file"));
    let program = serde_json::from_reader(filereader).expect("Failed to parse Renoir program");

    let RenoirProgram {
        jobs,
        // configuration,
        async_mode,
    } = program;

    let mut dependencies = Vec::new();
    let mut definitions = Vec::new();
    let mut post_execution = Vec::new();

    let jobs_statements: Vec<Vec<Vec<proc_macro2::TokenStream>>> = jobs
        .iter()
        .map(|job| {
            let source_nodes = find_source_nodes(job);
            if source_nodes.is_empty() {
                panic!("No source nodes found for job: {:?}", job);
            }

            source_nodes.iter().map(|source_node| {
                let mut job_statements = Vec::new();
                
                let stream_name = syn::Ident::new(source_node, proc_macro2::Span::call_site());
                let source = job.nodes.get(source_node).unwrap();
                match source {
                    RenoirNode::CsvSource { path, fields, types } => {
                        if fields.len() != types.len() {
                            panic!("Fields and types must have the same length for CSV source: {:?}", source);
                        }
                        let fields: Vec<proc_macro2::TokenStream> = fields.iter().map(|f| {
                            let field_ident = syn::Ident::new(f, proc_macro2::Span::call_site());
                            quote! { #field_ident }
                        }).collect();
                        let types: Vec<proc_macro2::TokenStream> = types.iter().map(|t| {
                            let type_ident = syn::Ident::new(t, proc_macro2::Span::call_site());
                            quote! { #type_ident }
                        }).collect();
                        dependencies.push(quote! {
                            use serde::Deserialize;
                        });
                        definitions.push(quote! {
                            #[derive(Debug, Clone, Deserialize)]
                            struct Row {
                                #(#fields: #types),*   
                            }
                        });
                        job_statements.push(quote! {
                            let source: CsvSource<Row> = CsvSource::new(#path);
                            let #stream_name = ctx.stream(source)
                        });
                    }
                    _ => panic!("{source_node} is not a source operator"),
                }

                let mut previous_node = source_node;
                while let Some(current_node) = job.edges.get(previous_node) {
                    let node = job.nodes.get(current_node).unwrap();
                    match node {
                        RenoirNode::Filter { condition } => {
                            let condition: proc_macro2::TokenStream = syn::parse_str(condition).expect("Failed to parse filter condition");
                            job_statements.push(quote! {
                                .filter(|row| #condition)
                            });
                        }
                        RenoirNode::Map { function } => {
                            let function: proc_macro2::TokenStream = syn::parse_str(function).expect("Failed to parse map function");
                            job_statements.push(quote! {
                                .map(|row| #function)
                            });
                        }
                        RenoirNode::CollectVec {} => {
                            job_statements.push(quote! {
                                .collect_vec();
                            });

                            post_execution.push(quote! {
                                println!("Collected results: {:?}", #stream_name.get());
                            });
                        }
                        _ => panic!("Unsupported or not implemented node type: {:?}", node),
                    }
                    previous_node = current_node;
                }

                job_statements
            }).collect()
        })
        .collect();

    let execute_statement = if async_mode {
        quote! {
            ctx.execute().await
        }
    } else {
        quote! {
            ctx.execute_blocking();
        }
    };

    let function = quote! {
        use renoir::prelude::*;
        #(#dependencies)*

        #(#definitions)*

        fn execute_environment() -> Result<(), Box<dyn std::error::Error>> {
            let (config, _) = RuntimeConfig::from_args();
            config.spawn_remote_workers();
            let mut ctx = StreamContext::new(config);

            #(
                #(
                    #(
                        #jobs_statements
                    )*
                )*
            )*

            #execute_statement

            #(#post_execution)*

            Ok(())
        }
    };

    TokenStream::from(function)
}
