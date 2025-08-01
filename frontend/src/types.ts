import { AlertColor, AlertPropsColorOverrides } from '@mui/material';
import { AxiosResponse } from 'axios';
import React, { Dispatch, ReactNode, SetStateAction } from 'react';
import { OverridableStringUnion } from '@mui/types';
import type { Node, Relationship } from '@neo4j-nvl/base';
import { NonOAuthError } from '@react-oauth/google';
import { BannerType } from '@neo4j-ndl/react';
import Queue from './utils/Queue';
import FileTable from './components/FileTable';

export interface CustomFileBase extends Partial<globalThis.File> {
  processingTotalTime: number | string;
  status: string;
  nodesCount: number;
  relationshipsCount: number;
  model: string;
  fileSource: string;
  sourceUrl?: string;
  wikiQuery?: string;
  gcsBucket?: string;
  gcsBucketFolder?: string;
  errorMessage?: string;
  uploadProgress?: number;
  processingStatus?: boolean;
  googleProjectId?: string;
  language?: string;
  processingProgress?: number;
  accessToken?: string;
  isChecked?: boolean;
  retryOptionStatus: boolean;
  retryOption: string;
  chunkNodeCount: number;
  chunkRelCount: number;
  entityNodeCount: number;
  entityEntityRelCount: number;
  communityNodeCount: number;
  communityRelCount: number;
  createdAt?: Date;
}
export interface CustomFile extends CustomFileBase {
  id: string;
}

export interface OptionType {
  readonly value: string;
  readonly label: string;
}

export type UserCredentials = {
  uri?: string;
  userName?: string;
  password?: string;
  database?: string;
  email: string;
  connection?: string;
} & { [key: string]: any };

export interface SourceNode extends Omit<CustomFileBase, 'relationshipsCount' | 'createdAt'> {
  fileName: string;
  fileSize: number;
  fileType: string;
  nodeCount?: number;
  processingTime: string;
  relationshipCount?: number;
  url?: string;
  awsAccessKeyId?: string;
  uploadprogress?: number;
  gcsProjectId?: string;
  processed_chunk?: number;
  total_chunks?: number;
  retry_condition?: string;
  createdAt: filedate;
}

export type ExtractParams = Pick<CustomFile, 'wikiQuery' | 'model' | 'sourceUrl' | 'language' | 'accessToken'> & {
  file?: File;
  aws_access_key_id?: string | null;
  aws_secret_access_key?: string | null;
  gcs_bucket_name?: string;
  gcs_bucket_folder?: string;
  gcs_blob_filename?: string;
  source_type?: string;
  file_name?: string;
  allowedNodes?: string[];
  allowedRelationship?: string[];
  gcs_project_id?: string;
  retry_condition: string;
  additional_instructions?: string;
} & { [key: string]: any };

export type UploadParams = {
  file: Blob;
  model: string;
  chunkNumber: number;
  totalChunks: number;
  originalname: string;
} & { [key: string]: any };

export type FormDataParams = ExtractParams | UploadParams;

export interface DropdownProps {
  onSelect: (option: OptionType | null | void) => void;
}

export interface CustomAlertProps {
  open: boolean;
  handleClose: () => void;
  alertMessage: string;
  severity?: OverridableStringUnion<AlertColor, AlertPropsColorOverrides> | undefined;
}
export interface DataComponentProps {
  openModal: () => void;
  isLargeDesktop?: boolean;
  isDisabled?: boolean;
}

export interface S3ModalProps {
  hideModal: () => void;
  open: boolean;
}
export interface GCSModalProps extends Omit<S3ModalProps, ''> {
  openGCSModal: () => void;
}

export interface SideNavProps {
  isExpanded: boolean;
  position: 'left' | 'right';
  toggleDrawer: () => void;
  deleteOnClick?: () => void;
  setShowDrawerChatbot?: Dispatch<SetStateAction<boolean>>;
  showDrawerChatbot?: boolean;
  setIsRightExpanded?: Dispatch<SetStateAction<boolean>>;
  messages?: Messages[];
  clearHistoryData?: boolean;
  toggles3Modal: () => void;
  toggleGCSModal: () => void;
  toggleGenericModal: () => void;
  setIsleftExpanded?: Dispatch<SetStateAction<boolean>>;
}

export interface DrawerProps {
  isExpanded: boolean;
  shows3Modal: boolean;
  showGCSModal: boolean;
  showGenericModal: boolean;
  toggleS3Modal: () => void;
  toggleGCSModal: () => void;
  toggleGenericModal: () => void;
}

export interface ContentProps {
  showChatBot: boolean;
  openChatBot: () => void;
  openTextSchema: () => void;
  openLoadSchema: () => void;
  openPredefinedSchema: () => void;
  showEnhancementDialog: boolean;
  toggleEnhancementDialog: () => void;
  setOpenConnection: Dispatch<SetStateAction<connectionState>>;
  showDisconnectButton: boolean;
  connectionStatus: boolean;
  combinedPatterns: string[];
  setCombinedPatterns: Dispatch<SetStateAction<string[]>>;
  combinedNodes: OptionType[];
  setCombinedNodes: Dispatch<SetStateAction<OptionType[]>>;
  combinedRels: OptionType[];
  setCombinedRels: Dispatch<SetStateAction<OptionType[]>>;
  openDataImporterSchema: () => void;
}

export interface FileTableProps {
  connectionStatus: boolean;
  setConnectionStatus: Dispatch<SetStateAction<boolean>>;
  onInspect: (id: string) => void;
  onChunkView: (name: string) => void;
  handleGenerateGraph: () => void;
  onRetry: (id: string) => void;
}

export interface CustomModalProps {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  submitLabel: string;
  submitHandler: () => void;
  statusMessage: string;
  status: 'unknown' | 'success' | 'info' | 'warning' | 'danger';
  setStatus: Dispatch<SetStateAction<'unknown' | 'success' | 'info' | 'warning' | 'danger'>>;
}

export interface CustomInput {
  value: string;
  label: string;
  placeHolder: string;
  onChangeHandler: React.ChangeEventHandler<HTMLInputElement>;
  submitHandler: (url: string) => void;
  disabledCheck: boolean;
  onCloseHandler: () => void;
  id: string;
  onBlurHandler: React.FocusEventHandler<HTMLInputElement>;
  status: 'unknown' | 'success' | 'info' | 'warning' | 'danger';
  setStatus: Dispatch<SetStateAction<'unknown' | 'success' | 'info' | 'warning' | 'danger'>>;
  statusMessage: string;
  isValid: boolean;
  isFocused: boolean;
  onPasteHandler: React.ClipboardEventHandler<HTMLInputElement>;
}

export interface CommonButtonProps {
  openModal: () => void;
  wrapperclassName?: string;
  logo: string;
  title?: string;
  className?: string;
  imgWidth?: number;
  imgeHeight?: number;
  isDisabled?: boolean;
}

export interface Source {
  page_numbers?: number[];
  source_name: string;
  start_time?: string;
}
export interface ChunkDetail {
  id: string;
  score: number;
}
export type ResponseMode = {
  message: string;
  sources?: string[];
  model?: string;
  total_tokens?: number;
  response_time?: number;
  cypher_query?: string;
  nodeDetails?: nodeDetailsProps;
  chunk_ids?: string[];
  graphonly_entities?: [];
  error?: string;
  entities?: string[];
  metric_question?: string;
  metric_contexts?: string;
  metric_answer?: string;
};
export interface Messages {
  id: number;
  user: string;
  datetime: string;
  isTyping?: boolean;
  isLoading?: boolean;
  speaking?: boolean;
  copying?: boolean;
  modes: {
    [key: string]: ResponseMode;
  };
  currentMode: string;
}

export type ChatbotProps = {
  messages: Messages[];
  setMessages: Dispatch<SetStateAction<Messages[]>>;
  isLoading: boolean;
  clear?: boolean;
  isFullScreen?: boolean;
  connectionStatus: boolean;
  isChatOnly?: boolean;
  isDeleteChatLoading: boolean;
};
export interface WikipediaModalTypes extends Omit<S3ModalProps, ''> {}

export interface GraphViewModalProps {
  open: boolean;
  inspectedName?: string;
  setGraphViewOpen: Dispatch<SetStateAction<boolean>>;
  viewPoint: string;
  nodeValues?: ExtendedNode[];
  relationshipValues?: ExtendedRelationship[];
  selectedRows?: CustomFile[] | undefined;
}

export type GraphType = 'Entities' | 'DocumentChunk' | 'Communities';

export type PartialLabelNode = Partial<Node> & {
  labels: string;
};

export interface CheckboxSectionProps {
  graphType: GraphType[];
  loading: boolean;
  handleChange: (graph: GraphType) => void;
  isCommunity: boolean;
  isDocChunk: boolean;
  isEntity: boolean;
}

export interface fileName {
  fileName: string;
  fileSize: number;
  url: string;
  gcsBucketName?: string;
  gcsBucketFolder?: string;
  status?: string;
  gcsProjectId: string;
  language?: string;
}
export interface URLSCAN_RESPONSE {
  status: string;
  success_count?: number;
  failed_count?: number;
  message: string;
  file_name?: fileName[];
  error?: string;
  file_source?: string;
  data?: any;
}
export interface statusAPI {
  status: string;
  message: string;
  file_name?: fileName;
}
export interface statusupdate {
  status: string;
  message: string;
  file_name: fileStatus;
}
export interface fileStatus {
  fileName: string;
  status: string;
  processingTime?: number;
  nodeCount?: number;
  relationshipCount?: number;
  model: string;
  total_chunks?: number;
  // total_pages?: number;
  processed_chunk?: number;
  chunkNodeCount: number;
  chunkRelCount: number;
  entityNodeCount: number;
  entityEntityRelCount: number;
  communityNodeCount: number;
  communityRelCount: number;
}
export interface PollingAPI_Response extends Partial<AxiosResponse> {
  data: statusupdate;
}
export interface ServerResponse extends Partial<AxiosResponse> {
  data: URLSCAN_RESPONSE;
}
export interface ScanProps {
  urlParam?: string;
  model?: string;
  accessKey?: string;
  secretKey?: string;
  wikiquery?: string;
  gcs_bucket_name?: string;
  gcs_bucket_folder?: string;
  source_type?: string;
  gcs_project_id?: string;
  access_token?: string;
}
export type alertStateType = {
  showAlert: boolean;
  alertType: OverridableStringUnion<AlertColor, AlertPropsColorOverrides> | undefined;
  alertMessage: string;
};
export interface BannerAlertProps extends Omit<alertStateType, 'alertType'> {
  alertType: BannerType;
}
export type Scheme = Record<string, string>;

export type LabelCount = Record<string, number>;

export interface LegendChipProps {
  scheme: Scheme;
  label: string;
  type: 'node' | 'relationship' | 'propertyKey';
  count?: number;
  onClick?: (e: React.MouseEvent<HTMLElement>) => void;
}
export interface FileContextProviderProps {
  children: ReactNode;
}
export interface orphanNode {
  id: string;
  elementId: string;
  description: string;
  labels: string[];
  embedding?: null | string;
}
export interface orphanNodeProps {
  documents: string[];
  chunkConnections: number;
  e: orphanNode;
  checked?: boolean;
  similar?: orphanNode[];
}
export interface labelsAndTypes {
  labels: string[];
  relationshipTypes: string[];
}
interface orphanTotalNodes {
  total: number;
}
export interface commonserverresponse {
  status: string;
  error?: string;
  message?: string | orphanTotalNodes;
  file_name?: string;
  data?:
    | OptionType
    | OptionType[]
    | string
    | string[]
    | uploadData
    | orphanNodeProps[]
    | dupNodes[]
    | { pageitems: chunkdata[]; total_pages: number }
    | { triplets: string[] };
}
export interface dupNodeProps {
  id: string;
  elementId: string;
  labels: string[];
  embedding?: null | string;
}
export interface dupNodes {
  e: dupNodeProps;
  similar: dupNodeProps[];
  documents: string[];
  chunkConnections: number;
}
export interface selectedDuplicateNodes {
  firstElementId: string;
  similarElementIds: string[];
}
export interface chunkdata {
  text: string;
  position: number;
  pagenumber: null | number;
}
export interface ScehmaFromText extends Partial<commonserverresponse> {
  data: {
    triplets: string[];
  };
}

export interface ServerData extends Partial<commonserverresponse> {
  data: {
    triplets: string[];
  };
}
export interface duplicateNodesData extends Partial<commonserverresponse> {
  data: dupNodes[];
}
export interface chunksData extends Partial<commonserverresponse> {
  data: { pageitems: chunkdata[]; total_pages: number };
}
export interface OrphanNodeResponse extends Partial<commonserverresponse> {
  data: orphanNodeProps[];
}
export type metricstate = {
  [key: string]: number | string;
} & {
  error: string;
};
export type metricdetails = Record<string, metricstate>;

export type multimodelmetric = {
  [key: string]: number | string;
} & {
  mode: string;
};
export interface MetricsResponse extends Omit<commonserverresponse, 'data'> {
  data: metricdetails;
}
export interface schema {
  nodelabels: string[];
  relationshipTypes: string[];
}

export interface SourceListServerData {
  data: SourceNode[];
  status: string;
  error?: string;
  message?: string;
}

export interface chatInfoMessage extends Partial<Messages> {
  sources: string[];
  model: string;
  response_time: number;
  total_tokens: number;
  mode: string;
  cypher_query?: string;
  graphonly_entities: [];
  error: string;
  entities_ids: string[];
  nodeDetails: nodeDetailsProps;
  metricquestion: string;
  metricanswer: string;
  metriccontexts: string;
  metricmodel: string;
  nodes: ExtendedNode[];
  relationships: ExtendedRelationship[];
  chunks: Chunk[];
  metricDetails:
    | {
        [key: string]: number | string;
      }
    | undefined;
  metricError: string;
  infoEntities: Entity[];
  communities: Community[];
  infoLoading: boolean;
  metricsLoading: boolean;
  activeChatmodes:
    | {
        [key: string]: ResponseMode;
      }
    | undefined;
  multiModelMetrics: multimodelmetric[];
  saveInfoEntitites: (entities: Entity[]) => void;
  saveNodes: (chatNodes: ExtendedNode[]) => void;
  saveChatRelationships: (chatRels: ExtendedRelationship[]) => void;
  saveChunks: (chatChunks: Chunk[]) => void;
  saveMetrics: (metricInfo: metricstate) => void;
  saveCommunities: (chatCommunities: Community[]) => void;
  toggleInfoLoading: React.DispatchWithoutAction;
  toggleMetricsLoading: React.DispatchWithoutAction;
  saveMultimodemetrics: (metrics: multimodelmetric[]) => void;
}

export interface eventResponsetypes extends Omit<SourceNode, 'total_chunks' | 'processingTime'> {
  total_chunks: number | null;
  processingTime: number;
}

export type Nullable<Type> = Type | null;

export type LabelColors = 'default' | 'success' | 'info' | 'warning' | 'danger' | undefined;

export interface HoverableLinkProps {
  url: string;
  children: React.ReactNode;
}

export interface ChunkEntitiesProps {
  userCredentials: UserCredentials | null;
  chunkIds: string[];
}

export interface CHATINFO_RESPONSE {
  status: string;
  message: string;
  error?: string;
  node: ExtendedNode[];
  relationships: Relationship[];
  data?: any;
}

export interface ChatInfo_APIResponse extends Partial<AxiosResponse> {
  data: CHATINFO_RESPONSE;
}

export interface nonoautherror extends NonOAuthError {
  message?: string;
}

export type Entity = {
  element_id: string;
  labels: string[];
  properties: {
    id: string;
  };
};
export type Community = {
  id: string;
  summary: string;
  weight: number;
  level: number;
  community_rank: number;
  score?: number;
  element_id: string;
};
export type GroupedEntity = {
  texts: Set<string>;
  color: string;
};

export interface uploadData {
  file_size: number;
  // total_pages: number;
  file_name: string;
  message: string;
}
export interface UploadResponse extends Partial<commonserverresponse> {
  data: uploadData;
}
export interface LargefilesProps {
  Files: CustomFile[];
  handleToggle: (ischecked: boolean, id: string) => void;
  checked: string[];
}

export interface MessagesContextProviderProps {
  children: ReactNode;
}

export interface GraphContextProviderProps {
  children: ReactNode;
}

export interface Chunk {
  id: string;
  position: number;
  text: string;
  fileName: string;
  length: number;
  embedding: string | null;
  page_number?: number;
  start_time?: string;
  content_offset?: string;
  url?: string;
  fileSource: string;
  score?: string;
  fileType: string;
  element_id: string;
}

export interface SpeechSynthesisProps {
  onEnd?: () => void;
}

export interface SpeechArgs {
  text?: string;
  rate?: number;
  pitch?: number;
  volume?: number;
}

// export interface SettingsModalProps {
//   open: boolean;
//   onClose: () => void;
//   openTextSchema: () => void;
//   openLoadSchema: () => void;
//   onContinue?: () => void;
//   settingView: 'contentView' | 'headerView';
//   isSchema?: boolean;
//   setIsSchema: Dispatch<SetStateAction<boolean>>;
//   onClear?: () => void;
// }
export interface Menuitems {
  id: string;
  title: string | JSX.Element;
  onClick: (e: React.MouseEvent<HTMLButtonElement, MouseEvent>) => void;
  disabledCondition: boolean;
  description?: string | React.ReactNode;
  isSelected?: boolean;
  selectedClassName?: string;
}
export type Vertical = 'top' | 'bottom';
export type Horizontal = 'left' | 'right' | 'center';
export interface Origin {
  vertical: Vertical;
  horizontal: Horizontal;
}

export type GraphStatsLabels = Record<
  string,
  {
    count: number;
    properties: Record<string, string>;
  }
>;

export interface ExtendedNode extends Node {
  labels: string[];
  properties: {
    fileName?: string;
    [key: string]: any;
  };
}

export interface NeoNode {
  element_id: string;
  labels: string[];
  properties: Record<string, any>;
}
export interface NeoRelationship {
  element_id: string;
  start_node_element_id: string;
  end_node_element_id: string;
  type: string;
}

export interface ExtendedRelationship extends Relationship {
  count?: number;
}
export interface connectionState {
  openPopUp: boolean;
  chunksExists: boolean;
  vectorIndexMisMatch: boolean;
  chunksExistsWithDifferentDimension: boolean;
}
export interface Message {
  type: 'success' | 'info' | 'warning' | 'danger' | 'unknown';
  content: string | React.ReactNode;
}

export interface ConnectionModalProps {
  open: boolean;
  setOpenConnection: Dispatch<SetStateAction<connectionState>>;
  setConnectionStatus: Dispatch<SetStateAction<boolean>>;
  isVectorIndexMatch: boolean;
  chunksExistsWithoutEmbedding: boolean;
  chunksExistsWithDifferentEmbedding: boolean;
  onSuccess?: () => void;
  isChatOnly?: boolean;
}
export interface ReusableDropdownProps extends DropdownProps {
  options: string[] | OptionType[];
  placeholder?: string;
  defaultValue?: string;
  children?: React.ReactNode;
  view?: 'ContentView' | 'GraphView';
  isDisabled: boolean;
  value?: OptionType;
}
export interface ChildRef {
  getSelectedRows: () => CustomFile[];
}
export interface IconProps {
  isFullScreen?: boolean;
  closeChatBot?: () => void;
  deleteOnClick?: () => void;
  messages: Messages[];
  isChatOnly?: boolean;
}
export interface S3File {
  fileName: string;
  fileSize: number;
  url: string;
}
export interface GraphViewButtonProps {
  nodeValues?: ExtendedNode[];
  relationshipValues?: ExtendedRelationship[];
  fill?: 'text' | 'filled' | 'outlined';
  label: string;
  viewType: string;
}
export interface DrawerChatbotProps {
  isExpanded: boolean;
  clearHistoryData: boolean;
  messages: Messages[];
  connectionStatus: boolean;
}

export interface ChatOnlyProps {
  clearHistoryData: boolean;
  messages: Messages[];
  connectionStatus: boolean;
  isReadOnlyUser: boolean;
}

export interface ContextProps {
  userCredentials: UserCredentials | null;
  setUserCredentials: (UserCredentials: UserCredentials) => void;
  isGdsActive: boolean;
  setGdsActive: Dispatch<SetStateAction<boolean>>;
  isReadOnlyUser: boolean;
  setIsReadOnlyUser: Dispatch<SetStateAction<boolean>>;
  connectionStatus: boolean;
  setConnectionStatus: Dispatch<SetStateAction<boolean>>;
  isBackendConnected: boolean;
  setIsBackendConnected: Dispatch<SetStateAction<boolean>>;
  errorMessage: string;
  setErrorMessage: Dispatch<SetStateAction<string>>;
  showDisconnectButton: boolean;
  setShowDisconnectButton: Dispatch<SetStateAction<boolean>>;
  isGCSActive: boolean;
  setIsGCSActive: Dispatch<SetStateAction<boolean>>;
  // chunksToBeProces: number;
  // setChunksToBeProces: Dispatch<SetStateAction<number>>;
}
export interface MessageContextType {
  messages: Messages[] | [];
  setMessages: Dispatch<SetStateAction<Messages[]>>;
  clearHistoryData: boolean;
  setClearHistoryData: Dispatch<SetStateAction<boolean>>;
  isDeleteChatLoading: boolean;
  setIsDeleteChatLoading: Dispatch<SetStateAction<boolean>>;
}

export interface GraphContextType {
  loadingGraph: boolean;
  setLoadingGraph: Dispatch<SetStateAction<boolean>>;
}

export interface DatabaseStatusProps {
  isConnected: boolean;
  isGdsActive: boolean;
  uri?: string;
  database?: string;
}

export type SourcesProps = {
  loading: boolean;
  mode: string;
  sources: string[];
  chunks: Chunk[];
};

export type ChunkProps = {
  loading: boolean;
  chunks: Chunk[];
  mode: string;
};

export type EntitiesProps = {
  loading: boolean;
  mode: string;
  graphonly_entities: [];
  infoEntities: Entity[];
};

export type CommunitiesProps = {
  loading: boolean;
  communities: Community[];
  mode: string;

  // nodeValues: ExtendedNode[];
  // relationshipValues: ExtendedRelationship[];
};

export interface entity {
  id: string;
  score: number;
}

export interface community {
  id: string;
  score: number;
}

export interface nodeDetailsProps {
  chunkdetails?: ChunkDetail[];
  entitydetails?: entity[];
  communitydetails?: community[];
}

export type entityProps = {
  entityids: [];
  relationshipids: [];
};

export interface showTextFromSchemaDialogType {
  triggeredFrom: string;
  show: boolean;
  onApply?: (selectedPattern: string[], nodes: OptionType[], rels: OptionType[]) => void;
}

export interface schemaLoadDialogType {
  triggeredFrom: string;
  show: boolean;
  onApply?: (selectedPattern: string[], nodes: OptionType[], rels: OptionType[]) => void;
}

export interface predefinedSchemaDialogType {
  triggeredFrom: string;
  show: boolean;
  onApply?: (selectedPattern: string[], nodes: OptionType[], rels: OptionType[]) => void;
}

export interface dataImporterSchemaDialogType {
  triggeredFrom: string;
  show: boolean;
  onApply?: (selectedPattern: string[], nodes: OptionType[], rels: OptionType[]) => void;
}

export interface FileContextType {
  files: (File | null)[] | [];
  filesData: CustomFile[] | [];
  setFiles: Dispatch<SetStateAction<(File | null)[]>>;
  setFilesData: Dispatch<SetStateAction<CustomFile[]>>;
  model: string;
  setModel: Dispatch<SetStateAction<string>>;
  graphType: string;
  setGraphType: Dispatch<SetStateAction<string>>;
  selectedTokenChunkSize: number;
  setSelectedTokenChunkSize: Dispatch<SetStateAction<number>>;
  selectedChunk_overlap: number;
  setSelectedChunk_overlap: Dispatch<SetStateAction<number>>;
  selectedChunks_to_combine: number;
  setSelectedChunks_to_combine: Dispatch<SetStateAction<number>>;
  rowSelection: Record<string, boolean>;
  setRowSelection: React.Dispatch<React.SetStateAction<Record<string, boolean>>>;
  selectedRows: string[];
  setSelectedRows: React.Dispatch<React.SetStateAction<string[]>>;
  chatModes: string[];
  setchatModes: Dispatch<SetStateAction<string[]>>;
  postProcessingTasks: string[];
  setPostProcessingTasks: React.Dispatch<React.SetStateAction<string[]>>;
  queue: Queue<CustomFile>;
  setQueue: Dispatch<SetStateAction<Queue<CustomFile>>>;
  processedCount: number;
  setProcessedCount: Dispatch<SetStateAction<number>>;
  postProcessingVal: boolean;
  setPostProcessingVal: Dispatch<SetStateAction<boolean>>;
  additionalInstructions: string;
  setAdditionalInstructions: Dispatch<SetStateAction<string>>;
  // all nodes and all patterns
  allPatterns: string[];
  setAllPatterns: Dispatch<SetStateAction<string[]>>;
  selectedNodes: readonly OptionType[] | OptionType[];
  setSelectedNodes: Dispatch<SetStateAction<readonly OptionType[] | OptionType[]>>;
  selectedRels: readonly OptionType[] | OptionType[];
  setSelectedRels: Dispatch<SetStateAction<readonly OptionType[] | OptionType[]>>;
  selectedSchemas: readonly OptionType[];
  setSelectedSchemas: Dispatch<SetStateAction<readonly OptionType[]>>;
  // user defined schema
  userDefinedNodes: OptionType[];
  setUserDefinedNodes: Dispatch<SetStateAction<OptionType[] | OptionType[]>>;
  userDefinedRels: OptionType[];
  setUserDefinedRels: Dispatch<SetStateAction<OptionType[] | OptionType[]>>;
  userDefinedPattern: string[];
  setUserDefinedPattern: Dispatch<SetStateAction<string[]>>;
  // Load Existing schema from db
  schemaLoadDialog: schemaLoadDialogType;
  setSchemaLoadDialog: React.Dispatch<React.SetStateAction<schemaLoadDialogType>>;
  dbNodes: OptionType[];
  setDbNodes: Dispatch<SetStateAction<OptionType[]>>;
  dbRels: OptionType[];
  setDbRels: Dispatch<SetStateAction<OptionType[]>>;
  dbPattern: string[];
  setDbPattern: Dispatch<SetStateAction<string[]>>;
  // Predefined schema
  predefinedSchemaDialog: predefinedSchemaDialogType;
  setPredefinedSchemaDialog: React.Dispatch<React.SetStateAction<predefinedSchemaDialogType>>;
  preDefinedNodes: OptionType[];
  setPreDefinedNodes: Dispatch<SetStateAction<OptionType[]>>;
  preDefinedRels: OptionType[];
  setPreDefinedRels: Dispatch<SetStateAction<OptionType[]>>;
  preDefinedPattern: string[];
  setPreDefinedPattern: Dispatch<SetStateAction<string[]>>;
  selectedPreDefOption: OptionType | null;
  setSelectedPreDefOption: Dispatch<SetStateAction<OptionType | null>>;
  // schema from text
  showTextFromSchemaDialog: showTextFromSchemaDialogType;
  setShowTextFromSchemaDialog: React.Dispatch<React.SetStateAction<showTextFromSchemaDialogType>>;
  schemaValNodes: OptionType[];
  setSchemaValNodes: Dispatch<SetStateAction<OptionType[]>>;
  schemaValRels: OptionType[];
  setSchemaValRels: Dispatch<SetStateAction<OptionType[]>>;
  schemaTextPattern: string[];
  setSchemaTextPattern: Dispatch<SetStateAction<string[]>>;

  // source, type,target options
  sourceOptions: OptionType[];
  setSourceOptions: Dispatch<SetStateAction<OptionType[]>>;
  typeOptions: OptionType[];
  setTypeOptions: Dispatch<SetStateAction<OptionType[]>>;
  targetOptions: OptionType[];
  setTargetOptions: Dispatch<SetStateAction<OptionType[]>>;

  // importer defined schema
  dataImporterSchemaDialog: dataImporterSchemaDialogType;
  setDataImporterSchemaDialog: React.Dispatch<React.SetStateAction<dataImporterSchemaDialogType>>;
  importerNodes: OptionType[];
  setImporterNodes: Dispatch<SetStateAction<OptionType[]>>;
  importerRels: OptionType[];
  setImporterRels: Dispatch<SetStateAction<OptionType[]>>;
  importerPattern: string[];
  setImporterPattern: Dispatch<SetStateAction<string[]>>;
}
export declare type Side = 'top' | 'right' | 'bottom' | 'left';

export type EntityType = 'node' | 'relationship';

export type BasicRelationship = {
  id: string;
  to: string;
  from: string;
  type: string;
  caption: string;
};

export type BasicNode = {
  id: string;
  type: string;
  labels: string[];
  properties: Record<string, string>;
  propertyTypes: Record<string, string>;
};

export type PropertyValue = string | number | boolean | [];

export type GraphPropertiesTableProps = {
  propertiesWithTypes: {
    key: string;
    value: PropertyValue;
  }[];
};

export type GraphPropertiesPanelProps = {
  inspectedItem: BasicNode | BasicRelationship;
  newScheme: Scheme;
};

export type withId = {
  id: string;
};

export interface GraphViewHandlerProps {
  nodeValues?: ExtendedNode[];
  relationshipValues?: ExtendedRelationship[];
  fill?: 'text' | 'filled' | 'outlined';
  label?: string;
  viewType?: string;
  buttonLabel: string;
  graphonly_entities?: [];
  entityInfo?: Entity[];
  mode?: string;
}

export interface ChatProps {
  chatMessages: Messages[];
}

export interface filedate {
  _DateTime__date: {
    _Date__ordinal: number;
    _Date__year: number;
    _Date__month: number;
    _Date__day: number;
  };
  _DateTime__time: {
    _Time__ticks: number;
    _Time__hour: number;
    _Time__minute: number;
    _Time__second: number;
    _Time__nanosecond: number;
    _Time__tzinfo: null;
  };
}
export type FileTableHandle = React.ElementRef<typeof FileTable>;

export interface VisibilityProps {
  isVisible: boolean;
}
export interface HeaderProp {
  chatOnly?: boolean;
  deleteOnClick?: () => void;
  setOpenConnection?: Dispatch<SetStateAction<connectionState>>;
  showBackButton?: boolean;
}

export type PatternOption = {
  label: string;
  value: string;
};

export type TupleCreationProps = {
  selectedSource: OptionType | null;
  selectedType: OptionType | null;
  selectedTarget: OptionType | null;
  onPatternChange: (
    source: OptionType | OptionType[] | null,
    type: OptionType | OptionType[] | null,
    target: OptionType | OptionType[] | null
  ) => void;
  onAddPattern: () => void;
  selectedTupleOptions: readonly OptionType[];
};

export interface TupleType {
  value: string;
  label: string;
  source: string;
  target: string;
  type: string;
  sourceType?: string;
}

export interface SchemaViewModalProps {
  open: boolean;
  inspectedName?: string;
  setGraphViewOpen: Dispatch<SetStateAction<boolean>>;
  viewPoint: string;
  nodeValues?: ExtendedNode[] | OptionType[];
  relationshipValues?: ExtendedRelationship[] | string[] | OptionType[];
  selectedRows?: CustomFile[] | undefined;
  schemaLoading?: boolean;
  view?: string;
}

export type UserDefinedGraphSchema = {
  nodes: ExtendedNode[];
  relationships: ExtendedRelationship[];
  scheme: Scheme;
};

export interface SchemaSelectionProps {
  open: boolean;
  onClose: () => void;
  pattern: string[];
  nodes: OptionType[];
  rels: OptionType[];
  handleRemove: (pattern: string) => void;
  handleSchemaView: (view?: string) => void;
  loading: boolean;
  highlightPattern?: string;
  onApply: () => void;
  onCancel: () => void;
  view?: string;
  message?: string;
}
